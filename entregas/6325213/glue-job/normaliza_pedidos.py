"""Glue Job PySpark: normaliza pedidos desnormalizados em esquema estrela.

Fluxo: S3 raw (CSV) -> normalizacao (fato + 2 dimensoes) -> S3 gold (Parquet)
       -> metadados da execucao no DynamoDB.
"""

import sys
from datetime import datetime, timezone

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Imports especificos do Glue (nao existem no teste local).
try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
except ImportError:
    GlueContext = None
    Job = None
    getResolvedOptions = None


# ---------------------------------------------------------------------------
# Funcoes PURAS (logica de normalizacao), testaveis localmente.
# ---------------------------------------------------------------------------

def normalizar(df_raw: DataFrame) -> dict[str, DataFrame]:
    """Deriva fato_pedidos, dim_cliente e dim_produto a partir do df_raw."""
    colunas_texto = ["cliente_nome", "cliente_uf", "produto_nome", "categoria"]
    colunas_chave = ["pedido_id", "cliente_id", "produto_id"]

    # Strings vazias ou em branco viram nulo.
    df = df_raw
    for c in colunas_texto + colunas_chave:
        df = df.withColumn(
            c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c))
        )

    # FATO: descarta chaves nulas e quantidade nula ou <= 0.
    fato = (
        df.filter(
            F.col("pedido_id").isNotNull()
            & F.col("cliente_id").isNotNull()
            & F.col("produto_id").isNotNull()
            & F.col("quantidade").isNotNull()
            & (F.col("quantidade") > 0)
        )
        .select(
            "pedido_id", "data_pedido", "cliente_id", "produto_id",
            "preco_unitario", "quantidade", "valor_total",
        )
        .dropDuplicates(["pedido_id"])
    )

    # DIMENSOES: deduplica por chave ANTES de preencher DESCONHECIDO.
    dim_cliente = (
        df.filter(F.col("cliente_id").isNotNull())
        .groupBy("cliente_id")
        .agg(
            F.first("cliente_nome", ignorenulls=True).alias("cliente_nome"),
            F.first("cliente_uf", ignorenulls=True).alias("cliente_uf"),
        )
        .fillna("DESCONHECIDO", subset=["cliente_nome", "cliente_uf"])
    )

    dim_produto = (
        df.filter(F.col("produto_id").isNotNull())
        .groupBy("produto_id")
        .agg(
            F.first("produto_nome", ignorenulls=True).alias("produto_nome"),
            F.first("categoria", ignorenulls=True).alias("categoria"),
        )
        .fillna("DESCONHECIDO", subset=["produto_nome", "categoria"])
    )

    return {
        "fato_pedidos": fato,
        "dim_cliente": dim_cliente,
        "dim_produto": dim_produto,
    }


def montar_metadados(execution_id, dataset, linhas_lidas, linhas_gravadas, status) -> dict:
    """Monta o item de metadados da execucao para o DynamoDB."""
    return {
        "execution_id": str(execution_id),
        "data_hora": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dataset": dataset,
        "linhas_lidas": int(linhas_lidas),
        "linhas_gravadas": int(linhas_gravadas),
        "status": status,
    }


# ---------------------------------------------------------------------------
# Funcoes de I/O (AWS), separadas da logica pura.
# ---------------------------------------------------------------------------

def ler_raw(spark: SparkSession, raw_path: str) -> DataFrame:
    """Le o CSV desnormalizado do Bucket_Raw."""
    schema = StructType([
        StructField("pedido_id", StringType(), True),
        StructField("data_pedido", StringType(), True),
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
    return spark.read.option("header", True).schema(schema).csv(raw_path)


def escrever_gold(tabelas: dict[str, DataFrame], gold_path: str) -> None:
    """Grava fato (particionado por data_pedido) e dimensoes em Parquet."""
    base = gold_path.rstrip("/")
    (
        tabelas["fato_pedidos"].write.mode("overwrite")
        .partitionBy("data_pedido")
        .parquet(f"{base}/fato_pedidos/")
    )
    tabelas["dim_cliente"].write.mode("overwrite").parquet(f"{base}/dim_cliente/")
    tabelas["dim_produto"].write.mode("overwrite").parquet(f"{base}/dim_produto/")


def gravar_metadados_dynamo(item: dict, ddb_table: str) -> None:
    """Grava o item de metadados na tabela DynamoDB."""
    import boto3

    table = boto3.resource("dynamodb", region_name="us-east-1").Table(ddb_table)
    table.put_item(Item=item)


# ---------------------------------------------------------------------------
# main: orquestra o contrato de 6 passos.
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
