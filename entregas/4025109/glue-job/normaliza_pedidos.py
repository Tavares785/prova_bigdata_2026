"""
Job_Normalizacao — Glue Job PySpark

Lê o Dataset_Exemplo desnormalizado do Bucket_Raw, normaliza no Modelo_Dimensional_Alvo
(fato + 2 dimensões), grava em Parquet particionado no Bucket_Gold e registra os metadados
da execução no DynamoDB.

Fluxo (contrato de 6 passos):
    1. Ler argumentos do Glue (getResolvedOptions): RAW_PATH, GOLD_PATH, DDB_TABLE, DATASET_NAME.
    2. Ler o CSV desnormalizado do RAW_PATH e contar linhas_lidas.
    3. Tratar nulos/linhas inválidas nas colunas-chave.
    4. Normalizar em fato_pedidos + dim_cliente + dim_produto (DataFrames/SQL).
    5. Gravar cada tabela em Parquet particionado no GOLD_PATH.
    6. Montar e gravar o item de metadados no DynamoDB DDB_TABLE.

Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 8.5
"""

import sys
from datetime import datetime, timezone

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
except ImportError:
    GlueContext = None
    Job = None
    getResolvedOptions = None


# ---------------------------------------------------------------------------
# Constante sentinela para textos ausentes (Req 6.7)
# ---------------------------------------------------------------------------

DESCONHECIDO = "DESCONHECIDO"

_COLS_DIM_CLIENTE = ["cliente_id", "cliente_nome", "cliente_uf"]
_COLS_DIM_PRODUTO = ["produto_id", "produto_nome", "categoria"]
_COLS_FATO = [
    "pedido_id",
    "data_pedido",
    "cliente_id",
    "produto_id",
    "preco_unitario",
    "quantidade",
    "valor_total",
]


# ---------------------------------------------------------------------------
# Helpers internos de tratamento de dados inválidos (Req 6.7)
# ---------------------------------------------------------------------------

def _chave_ausente(coluna: str):
    """Condição: coluna textual nula ou vazia após trim."""
    col = F.col(coluna)
    return col.isNull() | (F.trim(col.cast("string")) == F.lit(""))


def _texto_ou_desconhecido(coluna: str):
    """Substitui nulos/vazios por DESCONHECIDO (Req 6.7)."""
    col = F.col(coluna)
    limpo = F.trim(col.cast("string"))
    return (
        F.when(col.isNull() | (limpo == F.lit("")), F.lit(DESCONHECIDO))
        .otherwise(limpo)
        .alias(coluna)
    )


# ---------------------------------------------------------------------------
# Funções PURAS (lógica de normalização) — testáveis localmente, sem AWS
# ---------------------------------------------------------------------------

def normalizar(df_raw: DataFrame) -> dict[str, DataFrame]:
    """Normaliza o DataFrame desnormalizado no Modelo_Dimensional_Alvo (esquema estrela).

    - dim_cliente: uma linha por cliente_id, com cliente_nome e cliente_uf.
    - dim_produto: uma linha por produto_id, com produto_nome e categoria.
    - fato_pedidos: uma linha por pedido_id, com FKs e medidas.

    Regra de dados inválidos (Req 6.7):
      - Descarta do fato linhas sem pedido_id/cliente_id/produto_id ou quantidade <= 0.
      - Nas dimensões, textos ausentes viram DESCONHECIDO (a linha é mantida).

    Requirements: 6.1, 6.2, 6.7
    """
    quantidade = F.col("quantidade").cast("int")

    condicao_validas = (
        ~_chave_ausente("pedido_id")
        & ~_chave_ausente("cliente_id")
        & ~_chave_ausente("produto_id")
        & quantidade.isNotNull()
        & (quantidade > F.lit(0))
    )
    df_validas = df_raw.where(condicao_validas)

    fato_pedidos = (
        df_validas.select(
            F.trim(F.col("pedido_id").cast("string")).alias("pedido_id"),
            F.col("data_pedido").cast("date").alias("data_pedido"),
            F.trim(F.col("cliente_id").cast("string")).alias("cliente_id"),
            F.trim(F.col("produto_id").cast("string")).alias("produto_id"),
            F.col("preco_unitario").cast("double").alias("preco_unitario"),
            F.col("quantidade").cast("int").alias("quantidade"),
            F.col("valor_total").cast("double").alias("valor_total"),
        )
        .dropDuplicates(["pedido_id"])
        .select(*_COLS_FATO)
    )

    dim_cliente = (
        df_raw.where(~_chave_ausente("cliente_id"))
        .select(
            F.trim(F.col("cliente_id").cast("string")).alias("cliente_id"),
            _texto_ou_desconhecido("cliente_nome"),
            _texto_ou_desconhecido("cliente_uf"),
        )
        .dropDuplicates(["cliente_id"])
        .select(*_COLS_DIM_CLIENTE)
    )

    dim_produto = (
        df_raw.where(~_chave_ausente("produto_id"))
        .select(
            F.trim(F.col("produto_id").cast("string")).alias("produto_id"),
            _texto_ou_desconhecido("produto_nome"),
            _texto_ou_desconhecido("categoria"),
        )
        .dropDuplicates(["produto_id"])
        .select(*_COLS_DIM_PRODUTO)
    )

    return {
        "fato_pedidos": fato_pedidos,
        "dim_cliente": dim_cliente,
        "dim_produto": dim_produto,
    }


def montar_metadados(execution_id, dataset, linhas_lidas, linhas_gravadas, status) -> dict:
    """Monta o item de metadados de uma execução para gravar no DynamoDB.

    Requirements: 6.5, 8.5
    """
    data_hora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "execution_id": str(execution_id),
        "data_hora": data_hora,
        "dataset": str(dataset),
        "linhas_lidas": int(linhas_lidas),
        "linhas_gravadas": int(linhas_gravadas),
        "status": str(status),
    }


# ---------------------------------------------------------------------------
# Funções de I/O (efeitos colaterais / AWS) — separadas da lógica pura
# ---------------------------------------------------------------------------

def ler_raw(spark: SparkSession, raw_path: str) -> DataFrame:
    """Lê o CSV desnormalizado do Bucket_Raw como DataFrame.

    Requirements: 6.3
    """
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(raw_path)
    )


def escrever_gold(tabelas: dict[str, DataFrame], gold_path: str) -> None:
    """Grava as tabelas normalizadas em Parquet no Bucket_Gold.

    Layout:
      - fato_pedidos particionado por data_pedido:
        s3://<gold>/fato_pedidos/data_pedido=YYYY-MM-DD/part-*.parquet
      - dim_cliente e dim_produto sem partição.

    Requirements: 6.4, 6.6
    """
    gold = gold_path.rstrip("/")

    tabelas["fato_pedidos"].write.mode("overwrite").partitionBy("data_pedido").parquet(
        f"{gold}/fato_pedidos/"
    )
    tabelas["dim_cliente"].write.mode("overwrite").parquet(f"{gold}/dim_cliente/")
    tabelas["dim_produto"].write.mode("overwrite").parquet(f"{gold}/dim_produto/")


def gravar_metadados_dynamo(item: dict, ddb_table: str) -> None:
    """Grava o item de metadados da execução na tabela DynamoDB via boto3.

    Requirements: 6.5, 8.5
    """
    import boto3
    from decimal import Decimal

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    tabela = dynamodb.Table(ddb_table)

    item_dynamo = {
        "execution_id": item["execution_id"],
        "data_hora": item["data_hora"],
        "dataset": item["dataset"],
        "linhas_lidas": Decimal(str(item["linhas_lidas"])),
        "linhas_gravadas": Decimal(str(item["linhas_gravadas"])),
        "status": item["status"],
    }

    tabela.put_item(Item=item_dynamo)


# ---------------------------------------------------------------------------
# main — orquestra o contrato de 6 passos
# ---------------------------------------------------------------------------

def main() -> None:
    """Ponto de entrada do Glue Job. Orquestra o contrato de 6 passos.

    Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 8.5
    """
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "RAW_PATH", "GOLD_PATH", "DDB_TABLE", "DATASET_NAME"],
    )
    raw_path = args["RAW_PATH"]
    gold_path = args["GOLD_PATH"]
    ddb_table = args["DDB_TABLE"]
    dataset_name = args["DATASET_NAME"]

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    execution_id = args["JOB_NAME"] + "-" + str(sc.applicationId)

    try:
        # Passo 2 — Ler o raw e contar linhas_lidas.
        df_raw = ler_raw(spark, raw_path)
        linhas_lidas = df_raw.count()

        # Passos 3 e 4 — Normalizar (tratamento de inválidos dentro de normalizar).
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
