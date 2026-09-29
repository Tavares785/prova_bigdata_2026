
"""
Job_Normalizacao — Glue Job PySpark.

Lê o Dataset_Exemplo desnormalizado do Bucket_Raw, normaliza no Modelo_Dimensional_Alvo
(fato + 2 dimensões), grava em Parquet particionado no Bucket_Gold e registra os metadados
da execução no DynamoDB.

Fluxo (contrato de 6 passos — ver design.md, Components (c)):
    1. Ler argumentos do Glue (getResolvedOptions): RAW_PATH, GOLD_PATH, DDB_TABLE, DATASET_NAME.
    2. Ler o CSV desnormalizado do RAW_PATH e contar linhas_lidas.               (Req 6.3)
    3. Tratar nulos/linhas inválidas nas colunas-chave.                          (Req 6.7)
    4. Normalizar em fato_pedidos + dim_cliente + dim_produto (DataFrames/SQL).  (Req 6.1, 6.2)
    5. Gravar cada tabela em Parquet particionado no GOLD_PATH.                  (Req 6.4, 6.6)
    6. Montar e gravar o item de metadados no DynamoDB DDB_TABLE.                (Req 6.5, 8.5)

IMPORTANTE (arquitetura de teste):
    - As funções PURAS (`normalizar`, `montar_metadados`) operam apenas sobre DataFrames/valores,
      sem tocar AWS, para poderem ser testadas localmente numa SparkSession (ver local-test/).
    - As funções de I/O (`ler_raw`, `escrever_gold`, `gravar_metadados_dynamo`) ficam SEPARADAS
      da lógica pura e só rodam no ambiente Glue/AWS.

Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 8.5
"""

import sys
from datetime import datetime, timezone

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Imports específicos do Glue — disponíveis no runtime do AWS Glue.
# No teste local eles não são usados (a lógica pura roda em SparkSession pura).
try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
except ImportError:  # ambiente local sem o SDK do Glue
    GlueContext = None
    Job = None
    getResolvedOptions = None


DESCONHECIDO = "DESCONHECIDO"


# ---------------------------------------------------------------------------
# Funções PURAS (lógica de normalização) — TESTÁVEIS localmente, sem AWS.
# ---------------------------------------------------------------------------

def _texto_ou_nulo(nome_coluna: str):
    """Trim na coluna e converte string vazia em NULL."""
    c = F.trim(F.col(nome_coluna).cast("string"))
    return F.when(c == "", F.lit(None)).otherwise(c)


def _dimensao(df: DataFrame, chave: str, atributos: list) -> DataFrame:
    """Constrói uma dimensão: 1 linha por chave, textos ausentes viram DESCONHECIDO."""
    return (
        df.withColumn(chave, _texto_ou_nulo(chave))
        .filter(F.col(chave).isNotNull())
        .groupBy(chave)
        # first(ignorenulls=True) evita escolher um valor nulo quando há outra linha preenchida
        .agg(*[F.first(_texto_ou_nulo(a), ignorenulls=True).alias(a) for a in atributos])
        .select(
            chave,
            *[F.coalesce(F.col(a), F.lit(DESCONHECIDO)).alias(a) for a in atributos],
        )
    )


def normalizar(df_raw: DataFrame) -> dict[str, DataFrame]:
    """Normaliza o DataFrame desnormalizado no Modelo_Dimensional_Alvo (esquema estrela).

    Deriva, a partir do `df_raw` (tabela ampla `pedidos_desnormalizado`), três DataFrames:
      - ``dim_cliente``: uma linha por `cliente_id` (chave única), com `cliente_nome`, `cliente_uf`.
      - ``dim_produto``: uma linha por `produto_id` (chave única), com `produto_nome`, `categoria`.
      - ``fato_pedidos``: uma linha por `pedido_id`, com FKs `cliente_id`/`produto_id` e as medidas
        `preco_unitario`, `quantidade`, `valor_total` e a coluna de partição `data_pedido`.

    Regra de dados inválidos (Req 6.7):
      - Descartar do fato as linhas sem `pedido_id`, `cliente_id` ou `produto_id`, ou com
        `quantidade` ausente/`<= 0`.
      - Nas dimensões, textos ausentes (`cliente_nome`, `cliente_uf`, `produto_nome`, `categoria`)
        viram ``"DESCONHECIDO"`` (a linha da dimensão é mantida).

    Args:
        df_raw: DataFrame desnormalizado lido do Bucket_Raw.

    Returns:
        dict com as chaves ``"fato_pedidos"``, ``"dim_cliente"`` e ``"dim_produto"``,
        cada uma mapeando para o respectivo DataFrame normalizado.

    Requirements: 6.1, 6.2, 6.7
    """
    # --- Dimensões -------------------------------------------------------
    dim_cliente = _dimensao(df_raw, "cliente_id", ["cliente_nome", "cliente_uf"])
    dim_produto = _dimensao(df_raw, "produto_id", ["produto_nome", "categoria"])

    # --- Fato ------------------------------------------------------------
    fato = (
        df_raw.select(
            _texto_ou_nulo("pedido_id").alias("pedido_id"),
            _texto_ou_nulo("cliente_id").alias("cliente_id"),
            _texto_ou_nulo("produto_id").alias("produto_id"),
            F.col("preco_unitario").cast("double").alias("preco_unitario"),
            F.col("quantidade").cast("int").alias("quantidade"),
            F.col("valor_total").cast("double").alias("valor_total"),
            F.to_date(F.col("data_pedido")).alias("data_pedido"),
        )
        # Req 6.7: descarta linhas sem chaves ou com quantidade inválida
        .filter(
            F.col("pedido_id").isNotNull()
            & F.col("cliente_id").isNotNull()
            & F.col("produto_id").isNotNull()
            & F.col("quantidade").isNotNull()
            & (F.col("quantidade") > 0)
        )
        # Partição precisa de data válida
        .filter(F.col("data_pedido").isNotNull())
        # valor_total ausente é recalculado a partir de preço x quantidade
        .withColumn(
            "valor_total",
            F.coalesce(F.col("valor_total"), F.col("preco_unitario") * F.col("quantidade")),
        )
        # uma linha por pedido_id
        .dropDuplicates(["pedido_id"])
    )

    return {
        "fato_pedidos": fato,
        "dim_cliente": dim_cliente,
        "dim_produto": dim_produto,
    }


def montar_metadados(
    execution_id: str,
    dataset: str,
    linhas_lidas: int,
    linhas_gravadas: int,
    status: str,
) -> dict:
    """Monta o item de metadados de uma execução para gravar no DynamoDB.

    O item segue o esquema do Requirement 8 (chave de partição `execution_id`):
      ``execution_id``, ``data_hora`` (ISO-8601), ``dataset``, ``linhas_lidas`` (N),
      ``linhas_gravadas`` (N) e ``status`` (``"SUCESSO"`` | ``"FALHA"``).

    Args:
        execution_id: ID único da execução (chave de partição).
        dataset: nome do dataset processado (ex.: ``"pedidos_desnormalizado"``).
        linhas_lidas: contagem de linhas lidas do Bucket_Raw.
        linhas_gravadas: contagem de linhas gravadas no fato (Bucket_Gold).
        status: status final da execução (``"SUCESSO"`` ou ``"FALHA"``).

    Returns:
        dict com os atributos do item de metadados de execução.

    Requirements: 6.5, 8.5
    """
    if status not in ("SUCESSO", "FALHA"):
        raise ValueError("O status deve ser 'SUCESSO' ou 'FALHA'.")

    return {
        "execution_id": execution_id,
        "data_hora": datetime.now(timezone.utc).isoformat(),
        "dataset": dataset,
        "linhas_lidas": int(linhas_lidas),
        "linhas_gravadas": int(linhas_gravadas),
        "status": status,
    }


# ---------------------------------------------------------------------------
# Funções de I/O (efeitos colaterais / AWS) — SEPARADAS da lógica pura.
# ---------------------------------------------------------------------------

def ler_raw(spark: SparkSession, raw_path: str) -> DataFrame:
    """Lê o CSV desnormalizado do Bucket_Raw como DataFrame.

    Args:
        spark: SparkSession ativa.
        raw_path: caminho do CSV no S3 (ex.: ``s3://<raw>/pedidos/``).

    Returns:
        DataFrame com o Dataset_Exemplo desnormalizado.

    Requirements: 6.3
    """
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(raw_path)
    )


def escrever_gold(tabelas: dict[str, DataFrame], gold_path: str) -> None:
    """Grava as tabelas normalizadas em Parquet no Bucket_Gold.

    Layout esperado (Req 6.6):
      - ``fato_pedidos`` particionado por ``data_pedido``:
        ``s3://<gold>/fato_pedidos/data_pedido=YYYY-MM-DD/part-*.parquet``.
      - ``dim_cliente`` e ``dim_produto`` sem partição:
        ``s3://<gold>/dim_cliente/`` e ``s3://<gold>/dim_produto/``.

    Args:
        tabelas: dict retornado por :func:`normalizar`.
        gold_path: prefixo do Bucket_Gold (ex.: ``s3://<gold>/``).

    Requirements: 6.4, 6.6
    """
    base = gold_path.rstrip("/")

    (
        tabelas["fato_pedidos"]
        .write.mode("overwrite")
        .partitionBy("data_pedido")
        .parquet(f"{base}/fato_pedidos/")
    )
    tabelas["dim_cliente"].write.mode("overwrite").parquet(f"{base}/dim_cliente/")
    tabelas["dim_produto"].write.mode("overwrite").parquet(f"{base}/dim_produto/")


def gravar_metadados_dynamo(item: dict, ddb_table: str) -> None:
    """Grava o item de metadados da execução na tabela DynamoDB.

    Args:
        item: dict retornado por :func:`montar_metadados`.
        ddb_table: nome da tabela DynamoDB de catálogo de execuções.

    Requirements: 6.5, 8.5
    """
    import boto3  # importado aqui: só necessário no ambiente AWS

    tabela = boto3.resource("dynamodb").Table(ddb_table)
    tabela.put_item(Item=item)


# ---------------------------------------------------------------------------
# main — orquestra o contrato de 6 passos (ver design.md, Components (c)).
# ---------------------------------------------------------------------------

def main() -> None:
    """Ponto de entrada do Glue Job. Orquestra o contrato de 6 passos.

    Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 8.5
    """
    # Passo 1 — Ler argumentos do Glue.
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "RAW_PATH", "GOLD_PATH", "DDB_TABLE", "DATASET_NAME"],
    )
    raw_path = args["RAW_PATH"]
    gold_path = args["GOLD_PATH"]
    ddb_table = args["DDB_TABLE"]
    dataset_name = args["DATASET_NAME"]

    # Inicialização do contexto Spark/Glue.
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # execution_id único da rodada (usado como chave de partição no DynamoDB).
    execution_id = args["JOB_NAME"] + "-" + str(sc.applicationId)

    try:
        # Passo 2 — Ler o raw e contar linhas_lidas.
        df_raw = ler_raw(spark, raw_path)
        linhas_lidas = df_raw.count()

        # Passo 3 — Tratamento de nulos/inválidos é aplicado dentro de normalizar() (Req 6.7).

        # Passo 4 — Normalizar em fato + dimensões.
        tabelas = normalizar(df_raw)
        # cache evita recalcular a normalização no count e na escrita
        tabelas["fato_pedidos"] = tabelas["fato_pedidos"].cache()
        linhas_gravadas = tabelas["fato_pedidos"].count()

        # Passo 5 — Gravar Parquet particionado no gold.
        escrever_gold(tabelas, gold_path)

        # Passo 6 — Montar e gravar metadados (SUCESSO).
        item = montar_metadados(
            execution_id=execution_id,
            dataset=dataset_name,
            linhas_lidas=linhas_lidas,
            linhas_gravadas=linhas_gravadas,
            status="SUCESSO",
        )
        gravar_metadados_dynamo(item, ddb_table)

    except Exception:
        # Em caso de falha, registrar metadados com status=FALHA para rastreabilidade (Req 8.4).
        item = montar_metadados(
            execution_id=execution_id,
            dataset=dataset_name,
            linhas_lidas=0,
            linhas_gravadas=0,
            status="FALHA",
        )
        gravar_metadados_dynamo(item, ddb_table)
        raise

    job.commit()


if __name__ == "__main__":
    main()