"""
Normalização de REFERÊNCIA (gabarito do Professor) — prova-big-data-aws.

Implementação **completa e correta** das funções puras `normalizar` e `montar_metadados`,
seguindo exatamente o mesmo contrato do esqueleto entregue ao aluno em
``prova/glue-job/normaliza_pedidos.py``.

Este módulo NÃO é entregue ao aluno como solução: ele serve de **gabarito** contra o qual os
testes de propriedade (``test_normalizacao.py``) rodam, garantindo que a prova é solucionável.

A lógica opera apenas sobre DataFrames/valores (sem tocar AWS), rodando em uma SparkSession
local (ver ``local-test/README.md``).

Modelo_Dimensional_Alvo (esquema estrela — design.md, Data Models):
    - ``dim_cliente``: uma linha por `cliente_id`, com `cliente_nome`, `cliente_uf`.
    - ``dim_produto``: uma linha por `produto_id`, com `produto_nome`, `categoria`.
    - ``fato_pedidos``: uma linha por `pedido_id`, com FKs `cliente_id`/`produto_id`, a coluna
      de partição `data_pedido` e as medidas `preco_unitario`, `quantidade`, `valor_total`.

Regra de tratamento de dados inválidos (design.md, Req 6.7):
    - Descarta do fato linhas sem `pedido_id`, `cliente_id` ou `produto_id`, ou com
      `quantidade` ausente / `<= 0`.
    - Nas dimensões, textos ausentes (`cliente_nome`, `cliente_uf`, `produto_nome`,
      `categoria`) viram ``"DESCONHECIDO"`` (a linha da dimensão é mantida).

Requirements: 6.1, 6.2, 6.4, 6.6, 6.7, 8.5
"""

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Valor sentinela para textos ausentes nas dimensões (Req 6.7).
DESCONHECIDO = "DESCONHECIDO"

# Colunas que compõem cada tabela do modelo estrela.
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


# ---------------------------------------------------------------------------
# Helpers puros de tratamento de dados inválidos (Req 6.7).
# ---------------------------------------------------------------------------

def _chave_ausente(coluna: str):
    """Condição booleana: chave textual nula ou vazia (após trim)."""
    col = F.col(coluna)
    return col.isNull() | (F.trim(col.cast("string")) == F.lit(""))


def _texto_ou_desconhecido(coluna: str):
    """Coluna com textos ausentes/vazios preenchidos como ``DESCONHECIDO`` (Req 6.7)."""
    col = F.col(coluna)
    limpo = F.trim(col.cast("string"))
    return (
        F.when(col.isNull() | (limpo == F.lit("")), F.lit(DESCONHECIDO))
        .otherwise(limpo)
        .alias(coluna)
    )


def _linhas_validas(df_raw: DataFrame) -> DataFrame:
    """Filtra as linhas que podem participar do fato (todas as chaves + quantidade válida).

    Descarta linhas sem `pedido_id`/`cliente_id`/`produto_id` (nulos ou vazios) ou com
    `quantidade` ausente/`<= 0` (Req 6.7).
    """
    quantidade = F.col("quantidade").cast("int")
    condicao_validas = (
        ~_chave_ausente("pedido_id")
        & ~_chave_ausente("cliente_id")
        & ~_chave_ausente("produto_id")
        & quantidade.isNotNull()
        & (quantidade > F.lit(0))
    )
    return df_raw.where(condicao_validas)


# ---------------------------------------------------------------------------
# Funções PURAS de normalização (mesmo contrato do esqueleto do aluno).
# ---------------------------------------------------------------------------

def normalizar(df_raw: DataFrame) -> dict[str, DataFrame]:
    """Normaliza o DataFrame desnormalizado no Modelo_Dimensional_Alvo (esquema estrela).

    Args:
        df_raw: DataFrame desnormalizado lido do Bucket_Raw (11 colunas do design).

    Returns:
        dict com as chaves ``"fato_pedidos"``, ``"dim_cliente"`` e ``"dim_produto"``.

    Requirements: 6.1, 6.2, 6.4, 6.6, 6.7
    """
    # Somente as linhas com chaves e quantidade válidas participam do modelo estrela.
    df_validas = _linhas_validas(df_raw)

    # fato_pedidos: uma linha por pedido_id. Tipagem explícita das medidas para consistência.
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

    # dim_cliente: uma linha por cliente_id (chave única). Textos ausentes -> DESCONHECIDO.
    # Considera apenas linhas com cliente_id presente (linha sem chave não vira dimensão).
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

    # dim_produto: uma linha por produto_id (chave única). Textos ausentes -> DESCONHECIDO.
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
    """Monta o item de metadados de uma execução para gravar no DynamoDB.

    Segue o esquema do Requirement 8 (chave de partição `execution_id`):
      ``execution_id``, ``data_hora`` (ISO-8601 UTC), ``dataset``, ``linhas_lidas`` (N),
      ``linhas_gravadas`` (N) e ``status`` (``"SUCESSO"`` | ``"FALHA"``).

    Args:
        execution_id: ID único da execução (chave de partição).
        dataset: nome do dataset processado (ex.: ``"pedidos_desnormalizado"``).
        linhas_lidas: contagem de linhas lidas do Bucket_Raw.
        linhas_gravadas: contagem de linhas gravadas no fato (Bucket_Gold).
        status: status final da execução (``"SUCESSO"`` ou ``"FALHA"``).

    Returns:
        dict com os atributos do item de metadados de execução.

    Requirements: 6.5, 8.5
    """
    data_hora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "execution_id": str(execution_id),
        "data_hora": data_hora,
        "dataset": str(dataset),
        "linhas_lidas": int(linhas_lidas),
        "linhas_gravadas": int(linhas_gravadas),
        "status": str(status),
    }
