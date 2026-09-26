"""Raw -> esquema estrela -> Parquet -> metadados no DynamoDB.

Daniel de Oliveira Tavares Junior — RA 6325032.
As funções de transformação também podem ser importadas sem o SDK do Glue.
"""

import logging
import sys
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

LOGGER = logging.getLogger(__name__)
REGIAO = "us-east-1"
COLUNAS_FATO = [
    "pedido_id", "data_pedido", "cliente_id", "produto_id",
    "preco_unitario", "quantidade", "valor_total",
]
COLUNAS_TEXTO = ["cliente_nome", "cliente_uf", "produto_nome", "categoria"]
SCHEMA_RAW = T.StructType([
    T.StructField("pedido_id", T.StringType()),
    T.StructField("data_pedido", T.StringType()),
    T.StructField("cliente_id", T.StringType()),
    T.StructField("cliente_nome", T.StringType()),
    T.StructField("cliente_uf", T.StringType()),
    T.StructField("produto_id", T.StringType()),
    T.StructField("produto_nome", T.StringType()),
    T.StructField("categoria", T.StringType()),
    T.StructField("preco_unitario", T.DoubleType()),
    T.StructField("quantidade", T.IntegerType()),
    T.StructField("valor_total", T.DoubleType()),
])


def _texto_ou_nulo(nome):
    valor = F.trim(F.col(nome).cast("string"))
    return F.when(F.length(valor) > 0, valor).otherwise(F.lit(None))


def _dimensao(base, chave, atributos):
    """Prefere a linha mais completa; desempata pelo conteúdo, sem ordem física."""
    completas = sum(F.col(nome).isNotNull().cast("int") for nome in atributos)
    ordem = [completas.desc()] + [F.col(nome).asc_nulls_last() for nome in atributos]
    janela = Window.partitionBy(chave).orderBy(*ordem)
    return (
        base.select(chave, *atributos)
        .withColumn("_ordem", F.row_number().over(janela))
        .where(F.col("_ordem") == 1).drop("_ordem")
        .fillna("DESCONHECIDO", subset=atributos)
    )


def normalizar(df_raw: DataFrame) -> dict[str, DataFrame]:
    """Descarta chaves/quantidades inválidas e gera dimensões dos itens válidos.

    Strings vazias ou só com espaços equivalem a nulos. As dimensões escolhem
    uma linha completa quando disponível; atributos ainda ausentes recebem
    DESCONHECIDO. Duplicatas de pedido_id têm desempate lexicográfico estável.
    valor_total é preservado do CSV, como nas consultas do enunciado.
    """
    faltantes = set(SCHEMA_RAW.fieldNames()) - set(df_raw.columns)
    if faltantes:
        raise ValueError(f"Colunas obrigatórias ausentes: {sorted(faltantes)}")

    base = df_raw
    for nome in ["pedido_id", "cliente_id", "produto_id", *COLUNAS_TEXTO]:
        base = base.withColumn(nome, _texto_ou_nulo(nome))
    for nome, tipo in [
        ("data_pedido", "date"), ("preco_unitario", "double"),
        ("quantidade", "int"), ("valor_total", "double"),
    ]:
        base = base.withColumn(nome, F.expr(f"try_cast({nome} as {tipo})"))
    validos = base.where(
        F.col("pedido_id").isNotNull()
        & F.col("cliente_id").isNotNull()
        & F.col("produto_id").isNotNull()
        & (F.col("quantidade") > 0)
    )

    janela = Window.partitionBy("pedido_id").orderBy(
        *[F.col(nome).asc_nulls_last() for nome in COLUNAS_FATO if nome != "pedido_id"]
    )
    fato = (
        validos.select(*COLUNAS_FATO)
        .withColumn("_ordem", F.row_number().over(janela))
        .where(F.col("_ordem") == 1).drop("_ordem")
    )
    return {
        "fato_pedidos": fato,
        "dim_cliente": _dimensao(validos, "cliente_id", ["cliente_nome", "cliente_uf"]),
        "dim_produto": _dimensao(validos, "produto_id", ["produto_nome", "categoria"]),
    }


def montar_metadados(execution_id, dataset, linhas_lidas, linhas_gravadas, status) -> dict:
    if status not in {"SUCESSO", "FALHA"}:
        raise ValueError("status deve ser SUCESSO ou FALHA")
    if not execution_id or not dataset:
        raise ValueError("execution_id e dataset são obrigatórios")
    for valor in (linhas_lidas, linhas_gravadas):
        if type(valor) is not int or valor < 0:
            raise ValueError("As contagens devem ser inteiros não negativos")
    if linhas_gravadas > linhas_lidas:
        raise ValueError("O fato não pode ter mais linhas do que o raw")
    return {
        "execution_id": execution_id,
        "data_hora": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "dataset": dataset,
        "linhas_lidas": linhas_lidas,
        "linhas_gravadas": linhas_gravadas,
        "status": status,
    }


def ler_raw(spark: SparkSession, raw_path: str) -> DataFrame:
    return (
        spark.read.schema(SCHEMA_RAW)
        .option("header", True).option("enforceSchema", False)
        .option("mode", "PERMISSIVE").option("encoding", "UTF-8")
        .option("ignoreLeadingWhiteSpace", True)
        .option("ignoreTrailingWhiteSpace", True)
        .csv(raw_path)
    )


def escrever_gold(tabelas: dict[str, DataFrame], gold_path: str) -> None:
    """Sobrescreve cada tabela: reruns não acumulam pedidos nem partições antigas.

    As três escritas não são uma transação; consultar após o job SUCCEEDED.
    """
    raiz = gold_path.rstrip("/")
    for nome in ("dim_cliente", "dim_produto"):
        tabelas[nome].write.mode("overwrite").option("compression", "snappy").parquet(f"{raiz}/{nome}/")
    (
        tabelas["fato_pedidos"].write.mode("overwrite")
        .option("partitionOverwriteMode", "static")
        .option("compression", "snappy").partitionBy("data_pedido")
        .parquet(f"{raiz}/fato_pedidos/")
    )


def gravar_metadados_dynamo(item: dict, ddb_table: str) -> None:
    import boto3

    boto3.resource("dynamodb", region_name=REGIAO).Table(ddb_table).put_item(Item=item)


def main() -> None:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "RAW_PATH", "GOLD_PATH", "DDB_TABLE", "DATASET_NAME"]
    )
    execution_id = str(uuid4())
    linhas_lidas = linhas_gravadas = 0
    df_raw = fato = None
    try:
        contexto = GlueContext(SparkContext.getOrCreate())
        spark = contexto.spark_session
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        job = Job(contexto)
        job.init(args["JOB_NAME"], args)

        df_raw = ler_raw(spark, args["RAW_PATH"]).cache()
        linhas_lidas = df_raw.count()
        tabelas = normalizar(df_raw)
        fato = tabelas["fato_pedidos"].cache()
        # Impede uma partição Hive nula caso um CSV futuro traga datas inválidas.
        if fato.where(F.col("data_pedido").isNull()).limit(1).count():
            raise ValueError("data_pedido ausente ou inválida em item válido")
        escrever_gold(tabelas, args["GOLD_PATH"])
        linhas_gravadas = fato.count()
        job.commit()
        gravar_metadados_dynamo(
            montar_metadados(execution_id, args["DATASET_NAME"], linhas_lidas, linhas_gravadas, "SUCESSO"),
            args["DDB_TABLE"],
        )
        LOGGER.info("Execução %s: %s lidas, %s gravadas", execution_id, linhas_lidas, linhas_gravadas)
    except Exception:
        LOGGER.exception("Falha na execução %s", execution_id)
        try:
            gravar_metadados_dynamo(
                montar_metadados(execution_id, args["DATASET_NAME"], linhas_lidas, linhas_gravadas, "FALHA"),
                args["DDB_TABLE"],
            )
        except Exception:
            # A indisponibilidade do DynamoDB não pode esconder a falha original.
            LOGGER.exception("Também não foi possível registrar FALHA no DynamoDB")
        raise
    finally:
        if fato is not None:
            fato.unpersist()
        if df_raw is not None:
            df_raw.unpersist()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
