"""Normalização de pedidos: funções testáveis localmente e I/O separado para Glue."""
import logging
import sys
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

LOGGER = logging.getLogger(__name__)
COLUNAS = ["pedido_id", "data_pedido", "cliente_id", "cliente_nome", "cliente_uf",
           "produto_id", "produto_nome", "categoria", "preco_unitario", "quantidade", "valor_total"]
COLUNAS_FATO = ["pedido_id", "data_pedido", "cliente_id", "produto_id",
                "preco_unitario", "quantidade", "valor_total"]
SCHEMA_CSV = StructType([StructField(c, StringType(), True) for c in COLUNAS])
DESCONHECIDO = "DESCONHECIDO"


def _dimensao(df, chave, atributos):
    # Escolhe um registro inteiro: não combina atributos de registros diferentes.
    base = df.where(F.col(chave).isNotNull()).select(chave, *atributos)
    completude = sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in atributos)
    ordem = [completude.desc()] + [F.col(c).asc_nulls_last() for c in atributos]
    janela = Window.partitionBy(chave).orderBy(*ordem)
    return (base.withColumn("_ordem", F.row_number().over(janela))
            .where(F.col("_ordem") == 1).drop("_ordem")
            .fillna(DESCONHECIDO, subset=atributos))


def normalizar(df_raw: DataFrame) -> dict[str, DataFrame]:
    if not isinstance(df_raw, DataFrame):
        raise TypeError("df_raw deve ser um DataFrame Spark")
    faltantes = sorted(set(COLUNAS) - set(df_raw.columns))
    if faltantes:
        raise ValueError(f"Colunas obrigatórias ausentes: {faltantes}")
    if len(df_raw.columns) != len(set(df_raw.columns)):
        raise ValueError("Nomes de colunas duplicados")
    # Apenas espaços nas bordas: não removemos caracteres internos de identificadores.
    df = df_raw.select(*[
        F.when(F.trim(F.col(c).cast("string")) == "", F.lit(None))
        .otherwise(F.trim(F.col(c).cast("string"))).alias(c) for c in COLUNAS
    ])
    clientes = _dimensao(df, "cliente_id", ["cliente_nome", "cliente_uf"])
    produtos = _dimensao(df, "produto_id", ["produto_nome", "categoria"])
    # try_cast evita estouro e falha por conteúdo malformado independentemente do ANSI mode.
    df = (df.withColumn("quantidade", F.when(F.col("quantidade").rlike(r"^[+-]?\d+$"),
                                            F.expr("try_cast(quantidade AS INT)")))
          .withColumn("data_pedido", F.expr("try_cast(data_pedido AS DATE)"))
          .withColumn("preco_unitario", F.expr("try_cast(preco_unitario AS DOUBLE)"))
          .withColumn("valor_total", F.expr("try_cast(valor_total AS DOUBLE)")))
    validas = df.where(F.col("pedido_id").isNotNull() & F.col("cliente_id").isNotNull()
                      & F.col("produto_id").isNotNull() & (F.col("quantidade") > 0))
    # Regra adicional explícita: data/medida inválida interrompe a execução; não há descarte silencioso.
    invalida = F.col("data_pedido").isNull()
    for c in ("preco_unitario", "valor_total"):
        invalida = invalida | F.col(c).isNull() | F.isnan(c) | (F.abs(F.col(c)) == float("inf"))
    if validas.where(invalida).limit(1).count():
        raise ValueError("Pedido com chaves/quantidade válidas tem data ou medida inválida")
    janela = Window.partitionBy("pedido_id").orderBy(*[
        F.col(c).asc_nulls_last() for c in COLUNAS_FATO if c != "pedido_id"])
    fato = (validas.select(*COLUNAS_FATO).withColumn("_ordem", F.row_number().over(janela))
            .where(F.col("_ordem") == 1).drop("_ordem"))
    return {"fato_pedidos": fato, "dim_cliente": clientes, "dim_produto": produtos}


def montar_metadados(execution_id, dataset, linhas_lidas, linhas_gravadas, status) -> dict:
    for nome, valor in (("execution_id", execution_id), ("dataset", dataset)):
        if not isinstance(valor, str) or not valor.strip():
            raise ValueError(f"{nome} deve ser texto preenchido")
    for valor in (linhas_lidas, linhas_gravadas):
        if isinstance(valor, bool) or not isinstance(valor, int) or valor < 0:
            raise ValueError("Contagens devem ser inteiros não negativos")
    if linhas_gravadas > linhas_lidas:
        raise ValueError("Fato não pode ter mais linhas que a entrada")
    if status not in ("SUCESSO", "FALHA"):
        raise ValueError("Status deve ser SUCESSO ou FALHA")
    return {"execution_id": execution_id.strip(),
            "data_hora": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "dataset": dataset.strip(), "linhas_lidas": linhas_lidas,
            "linhas_gravadas": linhas_gravadas, "status": status}


def ler_raw(spark: SparkSession, raw_path: str) -> DataFrame:
    if not raw_path or not raw_path.strip():
        raise ValueError("RAW_PATH vazio")
    return (spark.read.schema(SCHEMA_CSV).option("header", True)
            .option("enforceSchema", False).option("mode", "FAILFAST")
            .option("encoding", "UTF-8").csv(raw_path))


def escrever_gold(tabelas: dict[str, DataFrame], gold_path: str) -> None:
    if set(tabelas) != {"fato_pedidos", "dim_cliente", "dim_produto"}:
        raise ValueError("Esperadas exatamente fato_pedidos, dim_cliente e dim_produto")
    if not gold_path or not gold_path.strip():
        raise ValueError("GOLD_PATH vazio")
    destino = gold_path.rstrip("/")
    for nome in ("dim_cliente", "dim_produto", "fato_pedidos"):
        writer = tabelas[nome].write.mode("overwrite").option("compression", "snappy")
        if nome == "fato_pedidos":
            writer = writer.partitionBy("data_pedido")
        writer.parquet(f"{destino}/{nome}")


def gravar_metadados_dynamo(item: dict, ddb_table: str) -> None:
    import boto3
    from botocore.config import Config
    if not ddb_table or not ddb_table.strip():
        raise ValueError("DDB_TABLE vazio")
    dynamo = boto3.resource("dynamodb", region_name="us-east-1",
                            config=Config(retries={"mode": "standard", "max_attempts": 3}))
    dynamo.Table(ddb_table).put_item(Item=item)


def _validar_caminhos(raw_path, gold_path):
    raw, gold = urlparse(raw_path), urlparse(gold_path)
    if any(p.scheme != "s3" or not p.netloc or p.query or p.fragment for p in (raw, gold)):
        raise ValueError("RAW_PATH/GOLD_PATH devem ser caminhos s3:// válidos")
    if raw.netloc == gold.netloc:
        raise ValueError("Raw e gold devem usar buckets separados")


def main() -> None:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    logging.basicConfig(level=logging.INFO)
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_PATH", "GOLD_PATH", "DDB_TABLE", "DATASET_NAME"])
    execution_id = str(uuid.uuid4())
    linhas_lidas = linhas_gravadas = 0
    df_raw = None
    tabelas = {}
    try:
        _validar_caminhos(args["RAW_PATH"], args["GOLD_PATH"])
        contexto = GlueContext(SparkContext.getOrCreate())
        spark = contexto.spark_session
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        spark.conf.set("spark.sql.shuffle.partitions", "4")
        job = Job(contexto)
        job.init(args["JOB_NAME"], args)
        df_raw = ler_raw(spark, args["RAW_PATH"]).persist()
        linhas_lidas = df_raw.count()
        tabelas = {nome: df.persist() for nome, df in normalizar(df_raw).items()}
        total_fato = tabelas["fato_pedidos"].count()
        escrever_gold(tabelas, args["GOLD_PATH"])
        # Conta como gravado apenas depois de todas as gravações retornarem com sucesso.
        linhas_gravadas = total_fato
        job.commit()
        item = montar_metadados(execution_id, args["DATASET_NAME"], linhas_lidas, linhas_gravadas, "SUCESSO")
        gravar_metadados_dynamo(item, args["DDB_TABLE"])
        LOGGER.info("Execução concluída: %s", item)
    except Exception:
        LOGGER.exception("Falha na execução %s", execution_id)
        try:
            item = montar_metadados(execution_id, args["DATASET_NAME"], linhas_lidas, linhas_gravadas, "FALHA")
            gravar_metadados_dynamo(item, args["DDB_TABLE"])
        except Exception:
            LOGGER.exception("Falha adicional ao registrar metadados; preservando erro original")
        raise
    finally:
        for df in ([df_raw] if df_raw is not None else []) + list(tabelas.values()):
            try:
                df.unpersist()
            except Exception:
                LOGGER.exception("Não foi possível liberar cache Spark")


if __name__ == "__main__":
    main()
