"""
Job_Normalizacao — Glue Job PySpark.

Lê o Dataset_Exemplo desnormalizado do Bucket_Raw, normaliza no
Modelo_Dimensional_Alvo (fato + 2 dimensões), grava em Parquet
particionado no Bucket_Gold e registra os metadados da execução
no DynamoDB.

Fluxo:
    1. Ler argumentos do Glue:
       RAW_PATH, GOLD_PATH, DDB_TABLE, DATASET_NAME.

    2. Ler o CSV desnormalizado do RAW_PATH
       e contar linhas_lidas.

    3. Tratar nulos/linhas inválidas nas colunas-chave.

    4. Normalizar em:
       - fato_pedidos
       - dim_cliente
       - dim_produto

    5. Gravar as tabelas em Parquet no GOLD_PATH.

    6. Registrar os metadados da execução no DynamoDB.

Requirements:
    6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 8.5
"""

import sys
from datetime import datetime, timezone

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Imports específicos do AWS Glue.
#
# No teste local, a lógica pura pode ser executada somente com SparkSession.
# ---------------------------------------------------------------------------

try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions

except ImportError:
    GlueContext = None
    Job = None
    getResolvedOptions = None


# ---------------------------------------------------------------------------
# Colunas obrigatórias do Dataset RAW.
# ---------------------------------------------------------------------------

COLUNAS_OBRIGATORIAS = {
    "pedido_id",
    "data_pedido",
    "cliente_id",
    "cliente_nome",
    "cliente_uf",
    "produto_id",
    "produto_nome",
    "categoria",
    "preco_unitario",
    "quantidade",
    "valor_total",
}


# ---------------------------------------------------------------------------
# Funções auxiliares PURAS.
# ---------------------------------------------------------------------------

def validar_colunas(df: DataFrame) -> None:
    """
    Verifica se o dataset contém todas as colunas obrigatórias.
    """

    faltantes = sorted(
        COLUNAS_OBRIGATORIAS.difference(df.columns)
    )

    if faltantes:
        raise ValueError(
            "Dataset RAW sem colunas obrigatórias: "
            + ", ".join(faltantes)
        )


def id_valido(nome_coluna: str):
    """
    Considera válido um ID não nulo e não vazio após trim.
    """

    return (
        F.col(nome_coluna).isNotNull()
        & (
            F.trim(
                F.col(nome_coluna).cast("string")
            )
            != ""
        )
    )


def id_normalizado(nome_coluna: str):
    """
    Normaliza IDs convertendo para string e removendo
    espaços no início e no fim.
    """

    return F.trim(
        F.col(nome_coluna).cast("string")
    )


def texto_ou_desconhecido(nome_coluna: str):
    """
    Substitui texto nulo ou vazio por DESCONHECIDO.
    """

    return (
        F.when(
            F.col(nome_coluna).isNull()
            | (
                F.trim(
                    F.col(nome_coluna).cast("string")
                )
                == ""
            ),
            F.lit("DESCONHECIDO"),
        )
        .otherwise(
            F.trim(
                F.col(nome_coluna).cast("string")
            )
        )
    )


# ---------------------------------------------------------------------------
# Funções PURAS.
#
# Não acessam AWS.
# Podem ser testadas localmente com SparkSession.
# ---------------------------------------------------------------------------

def normalizar(df_raw: DataFrame) -> dict[str, DataFrame]:
    """
    Normaliza o DataFrame desnormalizado no Modelo Dimensional Alvo.

    Resultado:
        dim_cliente
        dim_produto
        fato_pedidos

    Regra de dados inválidos:

    Fato:
        - descartar pedido_id nulo/vazio;
        - descartar cliente_id nulo/vazio;
        - descartar produto_id nulo/vazio;
        - descartar quantidade nula;
        - descartar quantidade <= 0.

    Dimensões:
        - manter registros cujo ID seja válido;
        - substituir textos nulos/vazios por DESCONHECIDO.

    Requirements:
        6.1, 6.2, 6.7
    """

    validar_colunas(df_raw)


    # -----------------------------------------------------------------------
    # DIM_CLIENTE
    # -----------------------------------------------------------------------

    dim_cliente = (
        df_raw
        .where(
            id_valido("cliente_id")
        )
        .select(
            id_normalizado(
                "cliente_id"
            ).alias(
                "cliente_id"
            ),

            texto_ou_desconhecido(
                "cliente_nome"
            ).alias(
                "cliente_nome"
            ),

            texto_ou_desconhecido(
                "cliente_uf"
            ).alias(
                "cliente_uf"
            ),
        )
        .dropDuplicates(
            ["cliente_id"]
        )
    )


    # -----------------------------------------------------------------------
    # DIM_PRODUTO
    # -----------------------------------------------------------------------

    dim_produto = (
        df_raw
        .where(
            id_valido("produto_id")
        )
        .select(
            id_normalizado(
                "produto_id"
            ).alias(
                "produto_id"
            ),

            texto_ou_desconhecido(
                "produto_nome"
            ).alias(
                "produto_nome"
            ),

            texto_ou_desconhecido(
                "categoria"
            ).alias(
                "categoria"
            ),
        )
        .dropDuplicates(
            ["produto_id"]
        )
    )


    # -----------------------------------------------------------------------
    # LINHAS VÁLIDAS PARA A FATO
    # -----------------------------------------------------------------------

    df_validas = (
        df_raw
        .where(
            id_valido("pedido_id")
            & id_valido("cliente_id")
            & id_valido("produto_id")
            & F.col("quantidade").isNotNull()
            & (
                F.col("quantidade")
                .cast("int")
                > 0
            )
        )
    )


    # -----------------------------------------------------------------------
    # FATO_PEDIDOS
    #
    # Ordem aderente ao Modelo Dimensional Alvo:
    #
    # pedido_id
    # data_pedido
    # cliente_id
    # produto_id
    # preco_unitario
    # quantidade
    # valor_total
    # -----------------------------------------------------------------------

    fato_pedidos = (
        df_validas
        .select(
            id_normalizado(
                "pedido_id"
            ).alias(
                "pedido_id"
            ),

            F.to_date(
                F.col("data_pedido")
            ).alias(
                "data_pedido"
            ),

            id_normalizado(
                "cliente_id"
            ).alias(
                "cliente_id"
            ),

            id_normalizado(
                "produto_id"
            ).alias(
                "produto_id"
            ),

            F.col(
                "preco_unitario"
            )
            .cast("double")
            .alias(
                "preco_unitario"
            ),

            F.col(
                "quantidade"
            )
            .cast("int")
            .alias(
                "quantidade"
            ),

            F.col(
                "valor_total"
            )
            .cast("double")
            .alias(
                "valor_total"
            ),
        )
        .dropDuplicates(
            ["pedido_id"]
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
    status,
) -> dict:
    """
    Monta o item de metadados de uma execução.

    Estrutura:
        execution_id
        data_hora
        dataset
        linhas_lidas
        linhas_gravadas
        status

    Requirements:
        6.5, 8.5
    """

    status_normalizado = str(
        status
    ).upper()

    if status_normalizado not in {
        "SUCESSO",
        "FALHA",
    }:
        raise ValueError(
            "status deve ser SUCESSO ou FALHA"
        )


    return {
        "execution_id": str(
            execution_id
        ),

        "data_hora": datetime.now(
            timezone.utc
        ).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),

        "dataset": str(
            dataset
        ),

        "linhas_lidas": int(
            linhas_lidas
        ),

        "linhas_gravadas": int(
            linhas_gravadas
        ),

        "status": status_normalizado,
    }


# ---------------------------------------------------------------------------
# Funções de I/O.
#
# Ficam separadas da lógica de transformação.
# ---------------------------------------------------------------------------

def ler_raw(
    spark: SparkSession,
    raw_path: str,
) -> DataFrame:
    """
    Lê o CSV desnormalizado do Bucket_Raw.

    Requirements:
        6.3
    """

    if not raw_path:
        raise ValueError(
            "RAW_PATH não pode ser vazio"
        )


    return (
        spark.read
        .option(
            "header",
            "true",
        )
        .option(
            "inferSchema",
            "true",
        )
        .option(
            "mode",
            "PERMISSIVE",
        )
        .csv(
            raw_path
        )
    )


def escrever_gold(
    tabelas: dict[str, DataFrame],
    gold_path: str,
) -> None:
    """
    Grava as tabelas normalizadas no Bucket_Gold.

    Layout esperado:

        s3://<gold>/fato_pedidos/
            data_pedido=YYYY-MM-DD/
                part-*.parquet

        s3://<gold>/dim_cliente/
            part-*.parquet

        s3://<gold>/dim_produto/
            part-*.parquet

    Requirements:
        6.4, 6.6
    """

    if not gold_path:
        raise ValueError(
            "GOLD_PATH não pode ser vazio"
        )


    tabelas_obrigatorias = {
        "fato_pedidos",
        "dim_cliente",
        "dim_produto",
    }


    faltantes = (
        tabelas_obrigatorias
        .difference(
            tabelas.keys()
        )
    )


    if faltantes:
        raise ValueError(
            "Tabelas ausentes para escrita no Gold: "
            + ", ".join(
                sorted(faltantes)
            )
        )


    base = gold_path.rstrip("/")


    # -----------------------------------------------------------------------
    # FATO
    # -----------------------------------------------------------------------

    (
        tabelas[
            "fato_pedidos"
        ]
        .write
        .mode(
            "overwrite"
        )
        .partitionBy(
            "data_pedido"
        )
        .parquet(
            f"{base}/fato_pedidos"
        )
    )


    # -----------------------------------------------------------------------
    # DIM_CLIENTE
    # -----------------------------------------------------------------------

    (
        tabelas[
            "dim_cliente"
        ]
        .write
        .mode(
            "overwrite"
        )
        .parquet(
            f"{base}/dim_cliente"
        )
    )


    # -----------------------------------------------------------------------
    # DIM_PRODUTO
    # -----------------------------------------------------------------------

    (
        tabelas[
            "dim_produto"
        ]
        .write
        .mode(
            "overwrite"
        )
        .parquet(
            f"{base}/dim_produto"
        )
    )


def gravar_metadados_dynamo(
    item: dict,
    ddb_table: str,
) -> None:
    """
    Grava os metadados da execução no DynamoDB.

    Requirements:
        6.5, 8.5
    """

    if not ddb_table:
        raise ValueError(
            "DDB_TABLE não pode ser vazio"
        )


    import boto3


    dynamodb = boto3.resource(
        "dynamodb"
    )


    tabela = dynamodb.Table(
        ddb_table
    )


    tabela.put_item(
        Item=item
    )


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Ponto de entrada do AWS Glue Job.

    Orquestra o contrato de seis passos.

    Requirements:
        6.1, 6.2, 6.3, 6.4,
        6.5, 6.6, 6.7, 8.5
    """


    if (
        getResolvedOptions is None
        or GlueContext is None
        or Job is None
    ):
        raise RuntimeError(
            "O main deve ser executado no AWS Glue. "
            "As funções puras podem ser testadas "
            "localmente com SparkSession."
        )


    # -----------------------------------------------------------------------
    # PASSO 1 — Argumentos do Glue.
    # -----------------------------------------------------------------------

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


    raw_path = args[
        "RAW_PATH"
    ]

    gold_path = args[
        "GOLD_PATH"
    ]

    ddb_table = args[
        "DDB_TABLE"
    ]

    dataset_name = args[
        "DATASET_NAME"
    ]


    # -----------------------------------------------------------------------
    # Inicialização Spark / Glue.
    # -----------------------------------------------------------------------

    sc = SparkContext.getOrCreate()


    glue_context = GlueContext(
        sc
    )


    spark = (
        glue_context
        .spark_session
    )


    job = Job(
        glue_context
    )


    job.init(
        args["JOB_NAME"],
        args,
    )


    execution_id = (
        f"{args['JOB_NAME']}-"
        f"{sc.applicationId}"
    )


    linhas_lidas = 0
    linhas_gravadas = 0


    try:

        # -------------------------------------------------------------------
        # PASSO 2 — Ler RAW.
        # -------------------------------------------------------------------

        df_raw = ler_raw(
            spark,
            raw_path,
        )


        linhas_lidas = (
            df_raw.count()
        )


        # -------------------------------------------------------------------
        # PASSOS 3 e 4
        #
        # Tratamento de inválidos + normalização.
        # -------------------------------------------------------------------

        tabelas = normalizar(
            df_raw
        )


        linhas_gravadas = (
            tabelas[
                "fato_pedidos"
            ]
            .count()
        )


        # -------------------------------------------------------------------
        # PASSO 5 — Escrever Gold.
        # -------------------------------------------------------------------

        escrever_gold(
            tabelas,
            gold_path,
        )


        # -------------------------------------------------------------------
        # PASSO 6 — Metadados de sucesso.
        # -------------------------------------------------------------------

        item = montar_metadados(
            execution_id=execution_id,
            dataset=dataset_name,
            linhas_lidas=linhas_lidas,
            linhas_gravadas=linhas_gravadas,
            status="SUCESSO",
        )


        gravar_metadados_dynamo(
            item,
            ddb_table,
        )


    except Exception:

        # -------------------------------------------------------------------
        # Registrar metadados de falha.
        # -------------------------------------------------------------------

        item = montar_metadados(
            execution_id=execution_id,
            dataset=dataset_name,
            linhas_lidas=linhas_lidas,
            linhas_gravadas=linhas_gravadas,
            status="FALHA",
        )


        try:

            gravar_metadados_dynamo(
                item,
                ddb_table,
            )

        except Exception as metadata_error:

            print(
                (
                    "Falha ao registrar metadados "
                    "de erro no DynamoDB: "
                    f"{metadata_error}"
                ),
                file=sys.stderr,
            )


        raise


    job.commit()


# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()