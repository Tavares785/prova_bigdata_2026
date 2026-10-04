"""
Job_Normalizacao — Glue Job PySpark (RA 6325269).

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
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType

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


RAW_SCHEMA = StructType([
    StructField("pedido_id", StringType(), True),
    StructField("data_pedido", DateType(), True),
    StructField("cliente_id", StringType(), True),
    StructField("cliente_nome", StringType(), True),
    StructField("cliente_uf", StringType(), True),
    StructField("produto_id", StringType(), True),
    StructField("produto_nome", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("preco_unitario", DoubleType(), True),
    StructField("quantidade", IntegerType(), True),
    StructField("valor_total", DoubleType(), True),
])


# ---------------------------------------------------------------------------
# Funções PURAS (lógica de normalização) — TESTÁVEIS localmente, sem AWS.
# ---------------------------------------------------------------------------

def esta_ausente(nome_coluna: str):
    c = F.col(nome_coluna)
    return c.isNull() | (F.trim(c) == F.lit(""))


def linhas_validas_fato(df_raw):
    pedido_valido = ~esta_ausente("pedido_id")
    cliente_valido = ~esta_ausente("cliente_id")
    produto_valido = ~esta_ausente("produto_id")
    quantidade_valida = (
        F.col("quantidade").cast("int").isNotNull()
        & (F.col("quantidade").cast("int") > F.lit(0))
    )

    return df_raw.where(
        (pedido_valido)
        & (cliente_valido)
        & (produto_valido)
        & (quantidade_valida)
    )


def tratar_texto_ausente(nome_coluna: str):
    return (
        F.when(esta_ausente(nome_coluna), F.lit(None).cast("string"))
        .otherwise(F.trim(F.col(nome_coluna)))
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
    df_validas = linhas_validas_fato(df_raw)
    fato = (
        df_validas.select(
            F.trim(F.col("pedido_id")).cast("string").alias("pedido_id"),
            F.col("data_pedido").cast("date").alias("data_pedido"),
            F.trim(F.col("cliente_id")).cast("string").alias("cliente_id"),
            F.trim(F.col("produto_id")).cast("string").alias("produto_id"),
            F.col("preco_unitario").cast("double").alias("preco_unitario"),
            F.col("quantidade").cast("int").alias("quantidade"),
            F.col("valor_total").cast("double").alias("valor_total"),
        )
        .dropDuplicates(["pedido_id"])
    )

    dim_cliente = (
        df_raw
        .where(~esta_ausente("cliente_id"))
        .select(
            F.trim(F.col("cliente_id")).alias("cliente_id"),
            tratar_texto_ausente("cliente_nome").alias("cliente_nome"),
            tratar_texto_ausente("cliente_uf").alias("cliente_uf"),
        )
        .groupBy("cliente_id")
        .agg(
            F.max("cliente_nome").alias("cliente_nome"),
            F.max("cliente_uf").alias("cliente_uf"),
        )
        .select(
            "cliente_id",
            F.coalesce(F.col("cliente_nome"), F.lit("DESCONHECIDO")).alias("cliente_nome"),
            F.coalesce(F.col("cliente_uf"), F.lit("DESCONHECIDO")).alias("cliente_uf"),
        )
    )

    dim_produto = (
        df_raw
        .where(~esta_ausente("produto_id"))
        .select(
            F.trim(F.col("produto_id")).alias("produto_id"),
            tratar_texto_ausente("produto_nome").alias("produto_nome"),
            tratar_texto_ausente("categoria").alias("categoria"),
        )
        .groupBy("produto_id")
        .agg(
            F.max("produto_nome").alias("produto_nome"),
            F.max("categoria").alias("categoria"),
        )
        .select(
            "produto_id",
            F.coalesce(F.col("produto_nome"), F.lit("DESCONHECIDO")).alias("produto_nome"),
            F.coalesce(F.col("categoria"), F.lit("DESCONHECIDO")).alias("categoria"),
        )
    )

    return {
        "fato_pedidos": fato,
        "dim_cliente": dim_cliente,
        "dim_produto": dim_produto,
    }


def montar_metadados(execution_id, dataset, linhas_lidas, linhas_gravadas, status) -> dict:
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
    data_hora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "execution_id": str(execution_id),
        "dataset": str(dataset),
        "linhas_lidas": int(linhas_lidas),
        "linhas_gravadas": int(linhas_gravadas),
        "status": str(status),
        "data_hora": data_hora,
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
    return spark.read.csv(raw_path, schema=RAW_SCHEMA, header=True, mode="PERMISSIVE")


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
    # Fato particionado por data_pedido (repartition -> 1 arquivo por partição).
    (
        tabelas["fato_pedidos"]
        .repartition("data_pedido")
        .write.mode("overwrite")
        .partitionBy("data_pedido")
        .parquet(base + "/fato_pedidos")
    )
    # Dimensões sem partição, 1 arquivo cada (volume pequeno).
    for nome in ("dim_cliente", "dim_produto"):
        tabelas[nome].coalesce(1).write.mode("overwrite").parquet(base + "/" + nome)


def gravar_metadados_dynamo(item: dict, ddb_table: str) -> None:
    """Grava o item de metadados da execução na tabela DynamoDB.

    Args:
        item: dict retornado por :func:`montar_metadados`.
        ddb_table: nome da tabela DynamoDB de catálogo de execuções.

    Requirements: 6.5, 8.5
    """
    import boto3
    boto3.resource("dynamodb").Table(ddb_table).put_item(Item=item)


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
    linhas_lidas = 0

    try:
        # Passo 2 — Ler o raw e contar linhas_lidas.
        df_raw = ler_raw(spark, raw_path)
        linhas_lidas = df_raw.count()

        # Passo 3 — Tratar nulos / linhas inválidas (Req 6.7).
        # A regra de descarte/DESCONHECIDO é aplicada dentro de normalizar() (função pura),
        # mantendo a lógica testável localmente.

        # Passo 4 — Normalizar em fato + dimensões.
        tabelas = normalizar(df_raw)
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
            linhas_lidas=linhas_lidas,
            linhas_gravadas=0,
            status="FALHA",
        )
        try:
            gravar_metadados_dynamo(item, ddb_table)
        except Exception as erro_ddb:
            print(f"AVISO: não foi possível gravar o registro de FALHA no DynamoDB: {erro_ddb}")
        raise

    job.commit()


if __name__ == "__main__":
    main()
