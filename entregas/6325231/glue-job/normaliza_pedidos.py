"""Normalização de pedidos: lógica Spark testável e integração Glue/AWS."""

import sys
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
except ImportError:  # testes locais não têm o runtime Glue
    GlueContext = Job = getResolvedOptions = None

RAW_SCHEMA = StructType([
    StructField("pedido_id", StringType()),
    StructField("data_pedido", DateType()),
    StructField("cliente_id", StringType()),
    StructField("cliente_nome", StringType()),
    StructField("cliente_uf", StringType()),
    StructField("produto_id", StringType()),
    StructField("produto_nome", StringType()),
    StructField("categoria", StringType()),
    StructField("preco_unitario", DoubleType()),
    StructField("quantidade", IntegerType()),
    StructField("valor_total", DoubleType()),
])


def _limpo(nome):
    return F.trim(F.col(nome).cast("string"))


def _presente(nome):
    return F.col(nome).isNotNull() & (_limpo(nome) != "")


def _texto(nome):
    valor = _limpo(nome)
    return F.when(F.col(nome).isNull() | (valor == ""), "DESCONHECIDO").otherwise(valor).alias(nome)


def _dimensao(df, chave, textos):
    """Mantém toda chave dimensional, mesmo se a linha não entrar no fato."""
    colunas = [F.trim(F.col(chave)).alias(chave)] + [_texto(nome) for nome in textos]
    base = df.where(_presente(chave)).select(*colunas)
    ordem = []
    for nome in textos:
        ordem += [F.when(F.col(nome) == "DESCONHECIDO", 1).otherwise(0), F.col(nome).asc()]
    janela = Window.partitionBy(chave).orderBy(*ordem)
    return base.withColumn("_rn", F.row_number().over(janela)).where(F.col("_rn") == 1).drop("_rn")


def normalizar(df_raw: DataFrame) -> dict[str, DataFrame]:
    """Produz uma linha por chave, dimensões completas e fato com chaves válidas."""
    clientes = _dimensao(df_raw, "cliente_id", ["cliente_nome", "cliente_uf"])
    produtos = _dimensao(df_raw, "produto_id", ["produto_nome", "categoria"])
    validas = df_raw.where(
        _presente("pedido_id") & _presente("cliente_id") & _presente("produto_id")
        & F.col("quantidade").cast("int").isNotNull()
        & (F.col("quantidade").cast("int") > 0)
    )
    fato_base = validas.select(
        _limpo("pedido_id").alias("pedido_id"),
        F.col("data_pedido").cast("date").alias("data_pedido"),
        _limpo("cliente_id").alias("cliente_id"),
        _limpo("produto_id").alias("produto_id"),
        F.col("preco_unitario").cast("double").alias("preco_unitario"),
        F.col("quantidade").cast("int").alias("quantidade"),
        F.col("valor_total").cast("double").alias("valor_total"),
    )
    # Em conflitos no mesmo pedido_id, a ordem completa escolhe sempre a mesma linha.
    ordem = [F.col(nome).asc_nulls_last() for nome in [
        "data_pedido", "cliente_id", "produto_id", "preco_unitario", "quantidade", "valor_total"
    ]]
    janela = Window.partitionBy("pedido_id").orderBy(*ordem)
    fato = fato_base.withColumn("_rn", F.row_number().over(janela)).where(F.col("_rn") == 1).drop("_rn")
    return {"fato_pedidos": fato, "dim_cliente": clientes, "dim_produto": produtos}


def montar_metadados(execution_id, dataset, linhas_lidas, linhas_gravadas, status) -> dict:
    if status not in {"SUCESSO", "FALHA"}:
        raise ValueError("status inválido")
    return {
        "execution_id": str(execution_id),
        "data_hora": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dataset": str(dataset),
        "linhas_lidas": int(linhas_lidas),
        "linhas_gravadas": int(linhas_gravadas),
        "status": status,
    }


def ler_raw(spark: SparkSession, raw_path: str) -> DataFrame:
    return spark.read.option("header", True).schema(RAW_SCHEMA).csv(raw_path)


def escrever_gold(tabelas: dict[str, DataFrame], gold_path: str) -> None:
    base = gold_path.rstrip("/") + "/"
    tabelas["dim_cliente"].write.mode("overwrite").parquet(base + "dim_cliente/")
    tabelas["dim_produto"].write.mode("overwrite").parquet(base + "dim_produto/")
    tabelas["fato_pedidos"].write.mode("overwrite").partitionBy("data_pedido").parquet(base + "fato_pedidos/")


def gravar_metadados_dynamo(item: dict, ddb_table: str) -> None:
    import boto3

    boto3.resource("dynamodb", region_name="us-east-1").Table(ddb_table).put_item(Item=item)


def main() -> None:
    if getResolvedOptions is None:
        raise RuntimeError("Runtime AWS Glue indisponível")
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_PATH", "GOLD_PATH", "DDB_TABLE", "DATASET_NAME"])
    sc = SparkContext.getOrCreate()
    context = GlueContext(sc)
    job = Job(context)
    job.init(args["JOB_NAME"], args)
    execution_id = args["JOB_NAME"] + "-" + str(uuid4())
    linhas_lidas = 0
    linhas_gravadas = 0
    try:
        bruto = ler_raw(context.spark_session, args["RAW_PATH"])
        linhas_lidas = bruto.count()
        tabelas = normalizar(bruto)
        fato = tabelas["fato_pedidos"]
        linhas_gravadas = fato.count()
        # Data e medidas inválidas não possuem regra de descarte: falhar explicitamente.
        if fato.where(
            F.col("data_pedido").isNull() | F.col("preco_unitario").isNull()
            | F.col("valor_total").isNull()
        ).limit(1).count():
            raise ValueError("Fato contém data ou medida inválida")
        escrever_gold(tabelas, args["GOLD_PATH"])
        item = montar_metadados(execution_id, args["DATASET_NAME"], linhas_lidas, linhas_gravadas, "SUCESSO")
        gravar_metadados_dynamo(item, args["DDB_TABLE"])
        job.commit()
    except Exception:
        item = montar_metadados(execution_id, args["DATASET_NAME"], linhas_lidas, 0, "FALHA")
        try:
            gravar_metadados_dynamo(item, args["DDB_TABLE"])
        except Exception as metadata_error:
            print(f"Falha ao registrar FALHA no DynamoDB: {type(metadata_error).__name__}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
