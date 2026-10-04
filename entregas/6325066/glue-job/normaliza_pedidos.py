import sys
import uuid
from datetime import datetime, timezone

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, StringType, StructField, StructType
)
import boto3

try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
except ImportError:
    GlueContext = None
    Job = None
    getResolvedOptions = None


DESCONHECIDO = "DESCONHECIDO"

SCHEMA_RAW = StructType([
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


def _texto_limpo(coluna: str):
    limpo = F.trim(F.col(coluna).cast("string"))
    return F.when(limpo != "", limpo)


def _dimensao(validas: DataFrame, chave: str, atributos: list[str]) -> DataFrame:
    agregados = [F.coalesce(F.max(a), F.lit(DESCONHECIDO)).alias(a) for a in atributos]
    return validas.groupBy(chave).agg(*agregados)


def normalizar(df_raw: DataFrame) -> dict[str, DataFrame]:
    df_clean = df_raw.withColumn("cliente_nome", _texto_limpo("cliente_nome")) \
                     .withColumn("cliente_uf", _texto_limpo("cliente_uf")) \
                     .withColumn("produto_nome", _texto_limpo("produto_nome")) \
                     .withColumn("categoria", _texto_limpo("categoria"))
    
    validas = df_clean.filter(
        F.col("pedido_id").isNotNull() &
        F.col("cliente_id").isNotNull() &
        F.col("produto_id").isNotNull() &
        F.col("data_pedido").isNotNull() &
        F.col("quantidade").isNotNull() &
        (F.col("quantidade") > 0)
    )

    dim_cliente = _dimensao(validas, "cliente_id", ["cliente_nome", "cliente_uf"])
    dim_produto = _dimensao(validas, "produto_id", ["produto_nome", "categoria"])

    fato_pedidos = validas.select(
        "pedido_id", "cliente_id", "produto_id", 
        "preco_unitario", "quantidade", "valor_total", "data_pedido"
    )

    return {
        "fato_pedidos": fato_pedidos,
        "dim_cliente": dim_cliente,
        "dim_produto": dim_produto
    }


def montar_metadados(execution_id, dataset, linhas_lidas, linhas_gravadas, status) -> dict:
    agora = datetime.now(timezone.utc).isoformat()
    return {
        "execution_id": execution_id,
        "data_hora": agora,
        "dataset": dataset,
        "linhas_lidas": linhas_lidas,
        "linhas_gravadas": linhas_gravadas,
        "status": status
    }


def ler_raw(spark: SparkSession, raw_path: str) -> DataFrame:
    return spark.read.csv(raw_path, header=True, schema=SCHEMA_RAW)


def escrever_gold(tabelas: dict[str, DataFrame], gold_path: str) -> None:
    tabelas["fato_pedidos"].write.mode("overwrite").partitionBy("data_pedido").parquet(f"{gold_path}fato_pedidos/")
    tabelas["dim_cliente"].write.mode("overwrite").parquet(f"{gold_path}dim_cliente/")
    tabelas["dim_produto"].write.mode("overwrite").parquet(f"{gold_path}dim_produto/")


def gravar_metadados_dynamo(item: dict, ddb_table: str) -> None:
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
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

    execution_id = args["JOB_NAME"] + "-" + str(sc.applicationId)
    linhas_lidas = 0

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
            linhas_lidas=linhas_lidas,
            linhas_gravadas=0,
            status="FALHA",
        )
        gravar_metadados_dynamo(item, ddb_table)
        raise

    job.commit()


if __name__ == "__main__":
    main()
