import sys
from datetime import datetime, timezone

import boto3

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, trim, when

try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
except ImportError:
    GlueContext = None
    Job = None
    getResolvedOptions = None


def normalizar(df_raw: DataFrame) -> dict[str, DataFrame]:

    # Dimensão cliente
    dim_cliente = (
        df_raw
        .filter(
            col("cliente_id").isNotNull()
            & (trim(col("cliente_id")) != "")
        )
        .select(
            col("cliente_id"),
            when(
                col("cliente_nome").isNull()
                | (trim(col("cliente_nome")) == ""),
                lit("DESCONHECIDO")
            ).otherwise(col("cliente_nome")).alias("cliente_nome"),
            when(
                col("cliente_uf").isNull()
                | (trim(col("cliente_uf")) == ""),
                lit("DESCONHECIDO")
            ).otherwise(col("cliente_uf")).alias("cliente_uf"),
        )
        .dropDuplicates(["cliente_id"])
    )

    # Dimensão produto
    dim_produto = (
        df_raw
        .filter(
            col("produto_id").isNotNull()
            & (trim(col("produto_id")) != "")
        )
        .select(
            col("produto_id"),
            when(
                col("produto_nome").isNull()
                | (trim(col("produto_nome")) == ""),
                lit("DESCONHECIDO")
            ).otherwise(col("produto_nome")).alias("produto_nome"),
            when(
                col("categoria").isNull()
                | (trim(col("categoria")) == ""),
                lit("DESCONHECIDO")
            ).otherwise(col("categoria")).alias("categoria"),
        )
        .dropDuplicates(["produto_id"])
    )

    # Fato
    fato_pedidos = (
        df_raw
        .filter(
            col("pedido_id").isNotNull()
            & (trim(col("pedido_id")) != "")
            & col("cliente_id").isNotNull()
            & (trim(col("cliente_id")) != "")
            & col("produto_id").isNotNull()
            & (trim(col("produto_id")) != "")
            & col("quantidade").isNotNull()
            & (col("quantidade") > 0)
        )
        .select(
            col("pedido_id"),
            col("data_pedido"),
            col("cliente_id"),
            col("produto_id"),
            col("preco_unitario"),
            col("quantidade"),
            col("valor_total"),
        )
    )

    return {
        "fato_pedidos": fato_pedidos,
        "dim_cliente": dim_cliente,
        "dim_produto": dim_produto,
    }


def montar_metadados(
    execution_id,
    dataset,
    linhas_lidas,
    linhas_gravadas,
    status
) -> dict:

    return {
        "execution_id": str(execution_id),
        "data_hora": datetime.now(timezone.utc).isoformat(),
        "dataset": str(dataset),
        "linhas_lidas": int(linhas_lidas),
        "linhas_gravadas": int(linhas_gravadas),
        "status": str(status),
    }


def ler_raw(spark: SparkSession, raw_path: str) -> DataFrame:

    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .csv(raw_path)
    )


def escrever_gold(
    tabelas: dict[str, DataFrame],
    gold_path: str
) -> None:

    base = gold_path.rstrip("/")

    (
        tabelas["fato_pedidos"]
        .write
        .mode("overwrite")
        .partitionBy("data_pedido")
        .parquet(f"{base}/fato_pedidos/")
    )

    (
        tabelas["dim_cliente"]
        .write
        .mode("overwrite")
        .parquet(f"{base}/dim_cliente/")
    )

    (
        tabelas["dim_produto"]
        .write
        .mode("overwrite")
        .parquet(f"{base}/dim_produto/")
    )


def gravar_metadados_dynamo(
    item: dict,
    ddb_table: str
) -> None:

    dynamodb = boto3.resource(
        "dynamodb",
        region_name="us-east-1"
    )

    table = dynamodb.Table(ddb_table)

    table.put_item(Item=item)


def main() -> None:

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "RAW_PATH",
            "GOLD_PATH",
            "DDB_TABLE",
            "DATASET_NAME",
        ],
    )

    raw_path = args["RAW_PATH"]
    gold_path = args["GOLD_PATH"]
    ddb_table = args["DDB_TABLE"]
    dataset_name = args["DATASET_NAME"]

    sc = SparkContext()

    glue_context = GlueContext(sc)

    spark = glue_context.spark_session

    job = Job(glue_context)

    job.init(
        args["JOB_NAME"],
        args
    )

    execution_id = (
        args["JOB_NAME"]
        + "-"
        + str(sc.applicationId)
    )

    try:

        # Ler RAW
        df_raw = ler_raw(
            spark,
            raw_path
        )

        linhas_lidas = df_raw.count()

        # Normalizar
        tabelas = normalizar(df_raw)

        linhas_gravadas = (
            tabelas["fato_pedidos"].count()
        )

        # Gravar GOLD
        escrever_gold(
            tabelas,
            gold_path
        )

        # Registrar execução
        item = montar_metadados(
            execution_id=execution_id,
            dataset=dataset_name,
            linhas_lidas=linhas_lidas,
            linhas_gravadas=linhas_gravadas,
            status="SUCESSO",
        )

        gravar_metadados_dynamo(
            item,
            ddb_table
        )

    except Exception:

        item = montar_metadados(
            execution_id=execution_id,
            dataset=dataset_name,
            linhas_lidas=0,
            linhas_gravadas=0,
            status="FALHA",
        )

        gravar_metadados_dynamo(
            item,
            ddb_table
        )

        raise

    job.commit()


if __name__ == "__main__":
    main()