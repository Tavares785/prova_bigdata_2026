"""
Job_Normalizacao - Glue Job PySpark.

Le o CSV desnormalizado do Bucket_Raw, normaliza em fato_pedidos + dim_cliente
+ dim_produto, grava Parquet no Bucket_Gold e registra metadados no DynamoDB.
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
except ImportError:  # ambiente local sem o SDK do Glue
    GlueContext = None
    Job = None
    getResolvedOptions = None


# ---------------------------------------------------------------------------
# Funcoes PURAS (sem AWS)
# ---------------------------------------------------------------------------

def _texto_limpo(nome_coluna):
    """Tira espacos e transforma texto vazio em NULL."""
    texto = F.trim(F.col(nome_coluna).cast("string"))
    return F.when(texto == "", F.lit(None)).otherwise(texto)


def normalizar(df_raw: DataFrame) -> dict[str, DataFrame]:
    """Normaliza o raw em fato_pedidos + dim_cliente + dim_produto."""

    # Dimensao cliente: uma linha por cliente_id.
    # first(ignorenulls=True) pega o primeiro valor NAO nulo do grupo.
    dim_cliente = (
        df_raw.filter(F.col("cliente_id").isNotNull())
        .groupBy("cliente_id")
        .agg(
            F.first(_texto_limpo("cliente_nome"), ignorenulls=True).alias("cliente_nome"),
            F.first(_texto_limpo("cliente_uf"), ignorenulls=True).alias("cliente_uf"),
        )
        .fillna("DESCONHECIDO", subset=["cliente_nome", "cliente_uf"])
    )

    # Dimensao produto: uma linha por produto_id.
    dim_produto = (
        df_raw.filter(F.col("produto_id").isNotNull())
        .groupBy("produto_id")
        .agg(
            F.first(_texto_limpo("produto_nome"), ignorenulls=True).alias("produto_nome"),
            F.first(_texto_limpo("categoria"), ignorenulls=True).alias("categoria"),
        )
        .fillna("DESCONHECIDO", subset=["produto_nome", "categoria"])
    )

    # Fato: descarta linhas sem chaves ou com quantidade ausente/<= 0.
    fato_pedidos = (
        df_raw.withColumn("quantidade", F.col("quantidade").cast("int"))
        .filter(
            F.col("pedido_id").isNotNull()
            & F.col("cliente_id").isNotNull()
            & F.col("produto_id").isNotNull()
        )
        .filter(F.col("quantidade").isNotNull() & (F.col("quantidade") > 0))
        .select(
            "pedido_id",
            "cliente_id",
            "produto_id",
            F.col("preco_unitario").cast("double").alias("preco_unitario"),
            "quantidade",
            F.col("valor_total").cast("double").alias("valor_total"),
            F.to_date(F.col("data_pedido")).alias("data_pedido"),
        )
        .dropDuplicates(["pedido_id"])
    )

    return {
        "fato_pedidos": fato_pedidos,
        "dim_cliente": dim_cliente,
        "dim_produto": dim_produto,
    }


def montar_metadados(execution_id, dataset, linhas_lidas, linhas_gravadas, status) -> dict:
    """Monta o item de metadados da execucao para o DynamoDB."""
    return {
        "execution_id": str(execution_id),
        "data_hora": datetime.now(timezone.utc).isoformat(),
        "dataset": str(dataset),
        "linhas_lidas": int(linhas_lidas),
        "linhas_gravadas": int(linhas_gravadas),
        "status": str(status),
    }


# ---------------------------------------------------------------------------
# Funcoes de I/O (so rodam no Glue/AWS)
# ---------------------------------------------------------------------------

def ler_raw(spark: SparkSession, raw_path: str) -> DataFrame:
    """Le o CSV desnormalizado do Bucket_Raw."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(raw_path)
    )


def escrever_gold(tabelas: dict[str, DataFrame], gold_path: str) -> None:
    """Grava as tabelas em Parquet no Bucket_Gold."""
    base = gold_path.rstrip("/")

    (
        tabelas["fato_pedidos"]
        .write.mode("overwrite")
        .partitionBy("data_pedido")
        .parquet(base + "/fato_pedidos/")
    )
    tabelas["dim_cliente"].write.mode("overwrite").parquet(base + "/dim_cliente/")
    tabelas["dim_produto"].write.mode("overwrite").parquet(base + "/dim_produto/")


def gravar_metadados_dynamo(item: dict, ddb_table: str) -> None:
    """Grava o item de metadados na tabela DynamoDB."""
    import boto3  # importado aqui para o teste local nao exigir boto3

    tabela = boto3.resource("dynamodb").Table(ddb_table)
    tabela.put_item(Item=item)


# ---------------------------------------------------------------------------
# main - contrato de 6 passos
# ---------------------------------------------------------------------------

def main() -> None:
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
        df_raw = ler_raw(spark, raw_path)
        linhas_lidas = df_raw.count()

        tabelas = normalizar(df_raw)
        linhas_gravadas = tabelas["fato_pedidos"].count()

        escrever_gold(tabelas, gold_path)

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
