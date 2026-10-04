"""
Testes de propriedade e de borda da normalização — prova-big-data-aws.

Este módulo valida a lógica de normalização de REFERÊNCIA (gabarito) localmente, antes de
subir ao Glue, usando uma ``SparkSession`` local (sem AWS) e a biblioteca **Hypothesis** para
gerar muitos DataFrames de entrada.

Infraestrutura de teste (Task 8.1):
    - Fixture de ``SparkSession`` local (escopo de sessão).
    - Estratégia Hypothesis que gera listas de linhas de pedido — com clientes/produtos que se
      repetem, datas variadas e linhas inválidas propositais (chaves ausentes, quantidade
      inválida) — e um helper que transforma essas linhas em um DataFrame Spark de entrada.
    - Importa a normalização de referência de ``normalizacao_referencia.py``.
    - ``@settings(max_examples=100)`` para as propriedades (mínimo 100 iterações).

As propriedades individuais (Properties 1–6) são adicionadas nas tasks 8.2–8.8.

Requirements: 6.1, 6.7
"""

import os
import shutil
import sys
import tempfile
from datetime import date

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Garante que o gabarito (mesmo diretório) seja importável independente do cwd do pytest.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from normalizacao_referencia import montar_metadados, normalizar  # noqa: E402

# Número mínimo de iterações por teste de propriedade (design.md, Testing Strategy).
MAX_EXAMPLES = 100

# Configuração padrão dos testes de propriedade (Properties 1–6, tasks 8.2–8.8):
#   - max_examples=100: mínimo de 100 iterações por propriedade (design.md).
#   - deadline=None: cada exemplo dispara ações Spark (jobs), que ultrapassam o deadline
#     padrão do Hypothesis (200ms); desabilitá-lo evita falsos negativos por tempo.
#   - suppress_health_check: gerar/coletar DataFrames Spark é naturalmente lento.
# Use como decorator nos testes de propriedade: ``@pbt_settings``.
pbt_settings = settings(
    max_examples=MAX_EXAMPLES,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)

# Esquema do DataFrame desnormalizado de entrada (11 colunas — design.md, Data Models).
# Os campos são nullable para permitir gerar linhas inválidas propositais (Req 6.7).
RAW_SCHEMA = StructType(
    [
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
    ]
)


# ---------------------------------------------------------------------------
# Fixture de SparkSession local (sem AWS).
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """SparkSession local compartilhada por toda a sessão de testes."""
    sessao = (
        SparkSession.builder.master("local[2]")
        .appName("prova-big-data-aws-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()


# ---------------------------------------------------------------------------
# Estratégias Hypothesis: geração de linhas de pedido.
# ---------------------------------------------------------------------------
# Conjuntos pequenos e repetidos de IDs/atributos forçam clientes e produtos a se repetirem
# entre linhas (exercita unicidade das dimensões — Property 2) e integridade referencial.

_CLIENTE_IDS = ["C1", "C2", "C3", "C4"]
_PRODUTO_IDS = ["P1", "P2", "P3", "P4"]
_UFS = ["SP", "RJ", "MG", "RS", "BA"]
_CATEGORIAS = ["eletronicos", "livros", "moda", "casa"]
_DATAS = [date(2026, 2, d) for d in range(1, 8)]  # datas variadas (uma semana)

# Textos que podem estar "ausentes" (None ou string vazia/espaços) para exercitar o
# preenchimento com DESCONHECIDO nas dimensões (Req 6.7).
_texto_opcional = st.one_of(
    st.none(),
    st.just(""),
    st.just("   "),
    st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ abcdefghijklmnopqrstuvwxyz", min_size=1, max_size=12),
)


@st.composite
def _linha_pedido(draw):
    """Gera uma linha de pedido como dict (pode ser válida ou inválida de propósito)."""
    cliente_id = draw(st.one_of(st.none(), st.just(""), st.sampled_from(_CLIENTE_IDS)))
    produto_id = draw(st.one_of(st.none(), st.just(""), st.sampled_from(_PRODUTO_IDS)))
    pedido_id = draw(st.one_of(st.none(), st.just(""), st.text(alphabet="0123456789", min_size=1, max_size=6)))
    # quantidade: inclui None e valores <= 0 (inválidos) além dos válidos (Req 6.7).
    quantidade = draw(st.one_of(st.none(), st.integers(min_value=-3, max_value=10)))
    preco_unitario = draw(st.floats(min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False))
    qtd_para_total = quantidade if quantidade is not None else 0
    return {
        "pedido_id": pedido_id,
        "data_pedido": draw(st.sampled_from(_DATAS)),
        "cliente_id": cliente_id,
        "cliente_nome": draw(_texto_opcional),
        "cliente_uf": draw(st.one_of(st.none(), st.sampled_from(_UFS))),
        "produto_id": produto_id,
        "produto_nome": draw(_texto_opcional),
        "categoria": draw(st.one_of(st.none(), st.sampled_from(_CATEGORIAS))),
        "preco_unitario": preco_unitario,
        "quantidade": quantidade,
        "valor_total": round(preco_unitario * qtd_para_total, 2),
    }


def linhas_pedido():
    """Estratégia Hypothesis: lista de linhas de pedido (pode conter linhas inválidas)."""
    return st.lists(_linha_pedido(), min_size=0, max_size=25)


# ---------------------------------------------------------------------------
# Helper: transforma linhas geradas em DataFrame Spark de entrada.
# ---------------------------------------------------------------------------

def linhas_para_df(spark: SparkSession, linhas: list[dict]) -> DataFrame:
    """Constrói o DataFrame desnormalizado de entrada a partir das linhas geradas.

    Aplica o ``RAW_SCHEMA`` de 11 colunas para garantir tipos consistentes mesmo quando a
    lista está vazia ou contém valores ausentes.
    """
    ordem = [c.name for c in RAW_SCHEMA.fields]
    dados = [tuple(linha[col] for col in ordem) for linha in linhas]
    return spark.createDataFrame(dados, schema=RAW_SCHEMA)


# ---------------------------------------------------------------------------
# Smoke test da infraestrutura (não é uma propriedade numerada).
# ---------------------------------------------------------------------------

def test_smoke_infra_normaliza(spark):
    """Sanidade: o helper + a normalização de referência funcionam de ponta a ponta."""
    linhas = [
        {
            "pedido_id": "1",
            "data_pedido": date(2026, 2, 1),
            "cliente_id": "C1",
            "cliente_nome": "Ana",
            "cliente_uf": "SP",
            "produto_id": "P1",
            "produto_nome": "Livro",
            "categoria": "livros",
            "preco_unitario": 10.0,
            "quantidade": 2,
            "valor_total": 20.0,
        },
        # Linha inválida proposital: sem cliente_id -> descartada do fato (Req 6.7).
        {
            "pedido_id": "2",
            "data_pedido": date(2026, 2, 2),
            "cliente_id": None,
            "cliente_nome": None,
            "cliente_uf": None,
            "produto_id": "P2",
            "produto_nome": "Fone",
            "categoria": "eletronicos",
            "preco_unitario": 50.0,
            "quantidade": 1,
            "valor_total": 50.0,
        },
    ]
    df_raw = linhas_para_df(spark, linhas)

    tabelas = normalizar(df_raw)

    assert set(tabelas.keys()) == {"fato_pedidos", "dim_cliente", "dim_produto"}
    # Só a primeira linha tem todas as chaves válidas -> 1 linha no fato.
    assert tabelas["fato_pedidos"].count() == 1

    meta = montar_metadados("exec-1", "pedidos_desnormalizado", df_raw.count(), 1, "SUCESSO")
    assert meta["execution_id"] == "exec-1"
    assert meta["linhas_lidas"] == 2
    assert meta["linhas_gravadas"] == 1
    assert meta["status"] == "SUCESSO"
    assert meta["data_hora"]  # campo preenchido


# ---------------------------------------------------------------------------
# Helpers puros (em Python) que replicam a regra de validade do design (Req 6.7).
# Usados para calcular o resultado esperado a partir das linhas geradas, sem
# depender da implementação de referência.
# ---------------------------------------------------------------------------

def _chave_valida(valor) -> bool:
    """Chave textual válida: não nula e não vazia após trim."""
    return valor is not None and str(valor).strip() != ""


def _quantidade_valida(valor) -> bool:
    """Quantidade válida: presente e > 0 (Req 6.7)."""
    return valor is not None and valor > 0


def _linhas_validas_esperadas(linhas: list[dict]) -> list[dict]:
    """Linhas de entrada que possuem todas as chaves obrigatórias + quantidade válida."""
    return [
        linha
        for linha in linhas
        if _chave_valida(linha["pedido_id"])
        and _chave_valida(linha["cliente_id"])
        and _chave_valida(linha["produto_id"])
        and _quantidade_valida(linha["quantidade"])
    ]


def _pedidos_validos_distintos(linhas: list[dict]) -> set[str]:
    """Conjunto de `pedido_id` (após trim) das linhas válidas — o grão do fato."""
    return {str(linha["pedido_id"]).strip() for linha in _linhas_validas_esperadas(linhas)}


# ---------------------------------------------------------------------------
# Property 1 — Preservação de contagem de linhas do fato (task 8.2).
# ---------------------------------------------------------------------------

# Feature: prova-big-data-aws, Property 1: contagem de fato_pedidos = linhas de entrada com chaves válidas
@given(linhas_pedido())
@pbt_settings
def test_prop1_contagem_fato_igual_linhas_validas(spark, linhas):
    """A contagem de fato_pedidos = nº de pedidos distintos com chaves e quantidade válidas.

    O grão do fato é `pedido_id` (dropDuplicates), então a contagem corresponde ao número de
    `pedido_id` distintos entre as linhas de entrada válidas (Req 6.4, 6.7).

    Validates: Requirements 6.4, 6.7
    """
    df_raw = linhas_para_df(spark, linhas)
    tabelas = normalizar(df_raw)

    esperado = len(_pedidos_validos_distintos(linhas))
    assert tabelas["fato_pedidos"].count() == esperado


# ---------------------------------------------------------------------------
# Property 2 — Unicidade das chaves das dimensões (task 8.3).
# ---------------------------------------------------------------------------

# Feature: prova-big-data-aws, Property 2: dim_cliente.cliente_id e dim_produto.produto_id únicos
@given(linhas_pedido())
@pbt_settings
def test_prop2_dimensoes_com_chaves_unicas(spark, linhas):
    """dim_cliente não repete `cliente_id` e dim_produto não repete `produto_id`.

    Validates: Requirements 6.1
    """
    df_raw = linhas_para_df(spark, linhas)
    tabelas = normalizar(df_raw)

    dim_cliente = tabelas["dim_cliente"]
    dim_produto = tabelas["dim_produto"]

    total_cliente = dim_cliente.count()
    distintos_cliente = dim_cliente.select("cliente_id").distinct().count()
    assert total_cliente == distintos_cliente

    total_produto = dim_produto.count()
    distintos_produto = dim_produto.select("produto_id").distinct().count()
    assert total_produto == distintos_produto


# ---------------------------------------------------------------------------
# Property 3 — Integridade referencial fato → dimensões (task 8.4).
# ---------------------------------------------------------------------------

# Feature: prova-big-data-aws, Property 3: toda FK do fato existe nas dimensões
@given(linhas_pedido())
@pbt_settings
def test_prop3_integridade_referencial(spark, linhas):
    """Todo `cliente_id`/`produto_id` do fato existe em dim_cliente/dim_produto.

    Validates: Requirements 6.1, 6.4
    """
    df_raw = linhas_para_df(spark, linhas)
    tabelas = normalizar(df_raw)

    fato = tabelas["fato_pedidos"]
    clientes_dim = {r["cliente_id"] for r in tabelas["dim_cliente"].select("cliente_id").collect()}
    produtos_dim = {r["produto_id"] for r in tabelas["dim_produto"].select("produto_id").collect()}

    for r in fato.select("cliente_id", "produto_id").collect():
        assert r["cliente_id"] in clientes_dim
        assert r["produto_id"] in produtos_dim


# ---------------------------------------------------------------------------
# Property 4 — Particionamento correto por data (task 8.5).
# ---------------------------------------------------------------------------

def _particoes_data_pedido(caminho_fato: str) -> set[str]:
    """Lê os diretórios de partição (`data_pedido=...`) gravados pelo Parquet."""
    particoes = set()
    for nome in os.listdir(caminho_fato):
        if nome.startswith("data_pedido="):
            particoes.add(nome.split("=", 1)[1])
    return particoes


# Feature: prova-big-data-aws, Property 4: partições gravadas = datas distintas do fato
@given(linhas_pedido())
@pbt_settings
def test_prop4_particoes_iguais_datas_distintas(spark, linhas):
    """Gravar fato_pedidos particionado por data_pedido produz uma partição por data distinta.

    Escreve em Parquet num diretório temporário único por exemplo e confere que os diretórios
    `data_pedido=...` correspondem exatamente ao conjunto de datas distintas do fato.

    Validates: Requirements 6.6, 9.6
    """
    df_raw = linhas_para_df(spark, linhas)
    fato = normalizar(df_raw)["fato_pedidos"]

    datas_esperadas = {
        r["data_pedido"].strftime("%Y-%m-%d")
        for r in fato.select("data_pedido").distinct().collect()
    }

    tmp = tempfile.mkdtemp(prefix="prop4_fato_")
    try:
        caminho_fato = os.path.join(tmp, "fato_pedidos")
        fato.write.mode("overwrite").partitionBy("data_pedido").parquet(caminho_fato)

        if datas_esperadas:
            assert _particoes_data_pedido(caminho_fato) == datas_esperadas
        else:
            # Fato vazio: nenhuma partição de dados deve ser criada.
            assert _particoes_data_pedido(caminho_fato) == set()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# Property 5 — Round-trip Parquet do fato (task 8.6).
# ---------------------------------------------------------------------------

def _linhas_fato_como_set(df: DataFrame) -> set:
    """Representa as linhas do fato como um conjunto de tuplas (chaves + medidas)."""
    return {
        (
            r["pedido_id"],
            r["data_pedido"],
            r["cliente_id"],
            r["produto_id"],
            r["preco_unitario"],
            r["quantidade"],
            r["valor_total"],
        )
        for r in df.select(
            "pedido_id",
            "data_pedido",
            "cliente_id",
            "produto_id",
            "preco_unitario",
            "quantidade",
            "valor_total",
        ).collect()
    }


# Feature: prova-big-data-aws, Property 5: releitura do Parquet preserva as linhas do fato
@given(linhas_pedido())
@pbt_settings
def test_prop5_roundtrip_parquet_preserva_linhas(spark, linhas):
    """Gravar fato_pedidos em Parquet e reler produz o mesmo conjunto de linhas.

    Compara chaves e medidas (mesmo conjunto de linhas) antes e depois do round-trip.

    Validates: Requirements 6.4
    """
    df_raw = linhas_para_df(spark, linhas)
    fato = normalizar(df_raw)["fato_pedidos"]
    esperado = _linhas_fato_como_set(fato)

    tmp = tempfile.mkdtemp(prefix="prop5_fato_")
    try:
        caminho_fato = os.path.join(tmp, "fato_pedidos")
        fato.write.mode("overwrite").partitionBy("data_pedido").parquet(caminho_fato)
        # Fornece o schema do fato na releitura: com fato vazio não há arquivos de dados e o
        # Spark não conseguiria inferir o schema (o conjunto relido é vazio, como esperado).
        relido = spark.read.schema(fato.schema).parquet(caminho_fato)
        assert _linhas_fato_como_set(relido) == esperado
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# Property 6 — Consistência dos metadados de execução (task 8.7).
# ---------------------------------------------------------------------------

# Feature: prova-big-data-aws, Property 6: linhas_lidas/linhas_gravadas corretos e campos preenchidos
@given(linhas_pedido())
@pbt_settings
def test_prop6_metadados_consistentes(spark, linhas):
    """montar_metadados registra linhas_lidas/linhas_gravadas corretos e campos preenchidos.

    `linhas_lidas` = contagem do DataFrame de entrada; `linhas_gravadas` = contagem do fato;
    `execution_id`, `data_hora`, `dataset` e `status` preenchidos.

    Validates: Requirements 6.5, 8.4, 8.5
    """
    df_raw = linhas_para_df(spark, linhas)
    fato = normalizar(df_raw)["fato_pedidos"]

    linhas_lidas = df_raw.count()
    linhas_gravadas = fato.count()

    meta = montar_metadados(
        "exec-prop6", "pedidos_desnormalizado", linhas_lidas, linhas_gravadas, "SUCESSO"
    )

    assert meta["linhas_lidas"] == linhas_lidas
    assert meta["linhas_gravadas"] == linhas_gravadas
    assert meta["execution_id"]
    assert meta["data_hora"]
    assert meta["dataset"]
    assert meta["status"]


# ---------------------------------------------------------------------------
# Testes de exemplo / borda (task 8.8) — casos concretos, não @given.
# ---------------------------------------------------------------------------

def _linha_base(**overrides) -> dict:
    """Constrói uma linha de pedido válida por padrão, com sobrescritas pontuais."""
    linha = {
        "pedido_id": "1",
        "data_pedido": date(2026, 2, 1),
        "cliente_id": "C1",
        "cliente_nome": "Ana",
        "cliente_uf": "SP",
        "produto_id": "P1",
        "produto_nome": "Livro",
        "categoria": "livros",
        "preco_unitario": 10.0,
        "quantidade": 2,
        "valor_total": 20.0,
    }
    linha.update(overrides)
    return linha


def test_borda_cliente_id_ausente_descartada_do_fato(spark):
    """cliente_id ausente → a linha é descartada do fato (Req 6.7)."""
    linhas = [
        _linha_base(pedido_id="1", cliente_id="C1"),
        _linha_base(pedido_id="2", cliente_id=None, cliente_nome=None),
    ]
    tabelas = normalizar(linhas_para_df(spark, linhas))
    pedidos = {r["pedido_id"] for r in tabelas["fato_pedidos"].select("pedido_id").collect()}
    assert pedidos == {"1"}


def test_borda_cliente_nome_ausente_vira_desconhecido(spark):
    """cliente_nome ausente → vira DESCONHECIDO na dim_cliente (Req 6.7)."""
    linhas = [_linha_base(cliente_id="C9", cliente_nome=None)]
    tabelas = normalizar(linhas_para_df(spark, linhas))
    linha_dim = tabelas["dim_cliente"].where("cliente_id = 'C9'").collect()
    assert len(linha_dim) == 1
    assert linha_dim[0]["cliente_nome"] == "DESCONHECIDO"


def test_borda_quantidade_invalida_descartada(spark):
    """quantidade <= 0 → a linha é descartada do fato (Req 6.7)."""
    linhas = [
        _linha_base(pedido_id="1", quantidade=3, valor_total=30.0),
        _linha_base(pedido_id="2", quantidade=0, valor_total=0.0),
        _linha_base(pedido_id="3", quantidade=-2, valor_total=-20.0),
    ]
    tabelas = normalizar(linhas_para_df(spark, linhas))
    pedidos = {r["pedido_id"] for r in tabelas["fato_pedidos"].select("pedido_id").collect()}
    assert pedidos == {"1"}


def test_borda_dataset_com_data_unica_gera_particao_unica(spark):
    """Dataset com uma única data_pedido → uma única partição no Parquet (Req 6.6, 9.6)."""
    linhas = [
        _linha_base(pedido_id="1", data_pedido=date(2026, 2, 5)),
        _linha_base(pedido_id="2", data_pedido=date(2026, 2, 5), produto_id="P2"),
    ]
    fato = normalizar(linhas_para_df(spark, linhas))["fato_pedidos"]

    tmp = tempfile.mkdtemp(prefix="borda_data_unica_")
    try:
        caminho_fato = os.path.join(tmp, "fato_pedidos")
        fato.write.mode("overwrite").partitionBy("data_pedido").parquet(caminho_fato)
        assert _particoes_data_pedido(caminho_fato) == {"2026-02-05"}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
