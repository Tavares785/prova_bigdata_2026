"""
Job_Normalizacao — Glue Job PySpark.

Lê o Dataset_Exemplo desnormalizado do Bucket_Raw, normaliza no Modelo_Dimensional_Alvo
(fato + 2 dimensões), grava em Parquet particionado no Bucket_Gold e registra os metadados
da execução no DynamoDB.
"""

import sys
from datetime import datetime, timezone

import boto3

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, coalesce, lit, trim, to_date
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Imports específicos do Glue
try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
except ImportError:
    GlueContext = None
    Job = None
    getResolvedOptions = None


# ---------------------------------------------------------------------------
# Funções PURAS
# ---------------------------------------------------------------------------

def normalizar(df_raw: DataFrame) -> dict[str, DataFrame]:
    """
    Normaliza o DataFrame desnormalizado em:

    - fato_pedidos
    - dim_cliente
    - dim_produto

    Regras:
    - pedido_id, cliente_id ou produto_id ausentes -> descarta do fato
    - quantidade ausente ou <= 0 -> descarta do fato
    - textos ausentes nas dimensões -> DESCONHECIDO
    """

    # ---------------------------------------------------------
    # 1. Normalização dos textos
    # ---------------------------------------------------------

    df = (
        df_raw
        .withColumn(
            "pedido_id",
            trim(col("pedido_id"))
        )
        .withColumn(
            "cliente_id",
            trim(col("cliente_id"))
        )
        .withColumn(
            "cliente_nome",
            coalesce(
                trim(col("cliente_nome")),
                lit("DESCONHECIDO")
            )
        )
        .withColumn(
            "cliente_uf",
            coalesce(
                trim(col("cliente_uf")),
                lit("DESCONHECIDO")
            )
        )
        .withColumn(
            "produto_id",
            trim(col("produto_id"))
        )
        .withColumn(
            "produto_nome",
            coalesce(
                trim(col("produto_nome")),
                lit("DESCONHECIDO")
            )
        )
        .withColumn(
            "categoria",
            coalesce(
                trim(col("categoria")),
                lit("DESCONHECIDO")
            )
        )
    )

    # ---------------------------------------------------------
    # 2. Dimensão Cliente
    # ---------------------------------------------------------

    dim_cliente = (
        df
        .filter(
            col("cliente_id").isNotNull()
            & (col("cliente_id") != "")
        )
        .select(
            "cliente_id",
            "cliente_nome",
            "cliente_uf"
        )
        .dropDuplicates(["cliente_id"])
    )

    # ---------------------------------------------------------
    # 3. Dimensão Produto
    # ---------------------------------------------------------

    dim_produto = (
        df
        .filter(
            col("produto_id").isNotNull()
            & (col("produto_id") != "")
        )
        .select(
            "produto_id",
            "produto_nome",
            "categoria"
        )
        .dropDuplicates(["produto_id"])
    )

    # ---------------------------------------------------------
    # 4. Fato Pedidos
    # ---------------------------------------------------------

    fato_pedidos = (
        df
        .filter(
            col("pedido_id").isNotNull()
            & (col("pedido_id") != "")
            & col("cliente_id").isNotNull()
            & (col("cliente_id") != "")
            & col("produto_id").isNotNull()
            & (col("produto_id") != "")
            & col("quantidade").isNotNull()
            & (col("quantidade") > 0)
        )
        .select(
            "pedido_id",
            "data_pedido",
            "cliente_id",
            "produto_id",
            "preco_unitario",
            "quantidade",
            "valor_total"
        )
        .dropDuplicates(["pedido_id"])
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
    """
    Monta o item de metadados para o DynamoDB.
    """

    data_hora = (
        datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )

    return {
        "execution_id": str(execution_id),
        "data_hora": data_hora,
        "dataset": str(dataset),
        "linhas_lidas": int(linhas_lidas),
        "linhas_gravadas": int(linhas_gravadas),
        "status": str(status),
    }


# ---------------------------------------------------------------------------
# Funções de I/O
# ---------------------------------------------------------------------------

def ler_raw(spark: SparkSession, raw_path: str) -> DataFrame:
    """
    Lê o CSV da camada raw.
    """

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

    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(raw_path)
    )

    return df.withColumn(
        "data_pedido",
        to_date(col("data_pedido"), "yyyy-MM-dd")
    )


def escrever_gold(
    tabelas: dict[str, DataFrame],
    gold_path: str
) -> None:
    """
    Grava as tabelas normalizadas em Parquet.

    fato_pedidos:
        particionado por data_pedido

    dim_cliente:
        sem partição

    dim_produto:
        sem partição
    """

    fato_pedidos = tabelas["fato_pedidos"]
    dim_cliente = tabelas["dim_cliente"]
    dim_produto = tabelas["dim_produto"]

    # Fato particionada por data
    (
        fato_pedidos
        .write
        .mode("overwrite")
        .partitionBy("data_pedido")
        .parquet(f"{gold_path.rstrip('/')}/fato_pedidos/")
    )

    # Dimensão cliente
    (
        dim_cliente
        .write
        .mode("overwrite")
        .parquet(f"{gold_path.rstrip('/')}/dim_cliente/")
    )

    # Dimensão produto
    (
        dim_produto
        .write
        .mode("overwrite")
        .parquet(f"{gold_path.rstrip('/')}/dim_produto/")
    )


def gravar_metadados_dynamo(
    item: dict,
    ddb_table: str
) -> None:
    """
    Grava o item de metadados no DynamoDB.
    """

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(ddb_table)

    table.put_item(Item=item)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Orquestra o pipeline completo.
    """

    # ---------------------------------------------------------
    # Passo 1 — Argumentos do Glue
    # ---------------------------------------------------------

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

    # ---------------------------------------------------------
    # Inicialização Spark / Glue
    # ---------------------------------------------------------

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)

    job.init(args["JOB_NAME"], args)

    execution_id = (
        args["JOB_NAME"]
        + "-"
        + str(sc.applicationId)
    )

    linhas_lidas = 0
    linhas_gravadas = 0

    try:

        # -----------------------------------------------------
        # Passo 2 — Ler RAW
        # -----------------------------------------------------

        df_raw = ler_raw(
            spark,
            raw_path
        )

        linhas_lidas = df_raw.count()

        # -----------------------------------------------------
        # Passos 3 e 4 — Tratar inválidos + Normalizar
        # -----------------------------------------------------

        tabelas = normalizar(df_raw)

        linhas_gravadas = (
            tabelas["fato_pedidos"]
            .count()
        )

        # -----------------------------------------------------
        # Passo 5 — Escrever GOLD
        # -----------------------------------------------------

        escrever_gold(
            tabelas,
            gold_path
        )

        # -----------------------------------------------------
        # Passo 6 — Metadados SUCESSO
        # -----------------------------------------------------

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

        job.commit()

    except Exception:

        # -----------------------------------------------------
        # Falha — registrar execução
        # -----------------------------------------------------

        item = montar_metadados(
            execution_id=execution_id,
            dataset=dataset_name,
            linhas_lidas=linhas_lidas,
            linhas_gravadas=linhas_gravadas,
            status="FALHA",
        )

        gravar_metadados_dynamo(
            item,
            ddb_table
        )

        raise


if __name__ == "__main__":
    main()