"""
Job_Normalizacao — Glue Job PySpark.

Lê o Dataset_Exemplo desnormalizado do Bucket_Raw, normaliza no Modelo_Dimensional_Alvo
(fato + 2 dimensões), grava em Parquet particionado no Bucket_Gold e registra os metadados
da execução no DynamoDB.
"""

from datetime import datetime, timezone
import sys

import boto3

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


def _chave_ausente(coluna: str):
    col = F.col(coluna)
    return col.isNull() | (F.trim(col.cast("string")) == F.lit(""))


def _texto_ou_desconhecido(coluna: str):
    col = F.col(coluna)
    limpo = F.trim(col.cast("string"))
    return (
        F.when(col.isNull() | (limpo == F.lit("")), F.lit(DESCONHECIDO))
        .otherwise(limpo)
        .alias(coluna)
    )


def _linhas_validas(df_raw: DataFrame) -> DataFrame:
    quantidade = F.col("quantidade").cast("int")
    condicao_validas = (
        ~_chave_ausente("pedido_id")
        & ~_chave_ausente("cliente_id")
        & ~_chave_ausente("produto_id")
        & quantidade.isNotNull()
        & (quantidade > F.lit(0))
    )
    return df_raw.where(condicao_validas)


def normalizar(df_raw: DataFrame) -> dict[str, DataFrame]:
    df_validas = _linhas_validas(df_raw)

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
    data_hora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "execution_id": str(execution_id),
        "data_hora": data_hora,
        "dataset": str(dataset),
        "linhas_lidas": int(linhas_lidas),
        "linhas_gravadas": int(linhas_gravadas),
        "status": str(status),
    }


def ler_raw(spark: SparkSession, raw_path: str) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(raw_path)
    )


def escrever_gold(tabelas: dict[str, DataFrame], gold_path: str) -> None:
    gold_path_clean = gold_path.rstrip("/")

    tabelas["fato_pedidos"].write.mode("overwrite").partitionBy("data_pedido").parquet(
        f"{gold_path_clean}/fato_pedidos"
    )

    tabelas["dim_cliente"].write.mode("overwrite").parquet(
        f"{gold_path_clean}/dim_cliente"
    )
    tabelas["dim_produto"].write.mode("overwrite").parquet(
        f"{gold_path_clean}/dim_produto"
    )


def gravar_metadados_dynamo(item: dict, ddb_table: str) -> None:
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table(ddb_table)
    table.put_item(Item=item)


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

    execution_id = f"{args['JOB_NAME']}-{sc.applicationId}"

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

    except Exception as e:
        item = montar_metadados(
            execution_id=execution_id,
            dataset=dataset_name,
            linhas_lidas=0,
            linhas_gravadas=0,
            status="FALHA",
        )
        gravar_metadados_dynamo(item, ddb_table)
        raise e

    job.commit()


if __name__ == "__main__":
    main()