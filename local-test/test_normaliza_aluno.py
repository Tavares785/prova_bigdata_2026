"""
Testes do Job do ALUNO (`glue-job/normaliza_pedidos.py`) contra o CSV real do dataset.

Diferente de `test_normalizacao.py` (que valida o gabarito do professor), estes testes importam
o script que vai para o Glue. Rodam com SparkSession local, sem AWS (boto3/Glue simulados).

Como rodar (a partir de `local-test/`, com o repositório inteiro visível no container):
    docker build -t prova-local .
    docker run --rm -v "$PWD/..":/repo -w /repo/local-test prova-local pytest -v test_normaliza_aluno.py
"""

import importlib.util
import sys
import types
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

RAIZ = Path(__file__).resolve().parent.parent
CSV = RAIZ / "dataset" / "pedidos_desnormalizado.csv"
SCRIPT = RAIZ / "glue-job" / "normaliza_pedidos.py"


def _carregar_job():
    spec = importlib.util.spec_from_file_location("normaliza_pedidos", SCRIPT)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


job = _carregar_job()


@pytest.fixture(scope="session")
def spark():
    sessao = (
        SparkSession.builder.master("local[2]")
        .appName("teste-aluno")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()


@pytest.fixture(scope="session")
def df_raw(spark):
    return job.ler_raw(spark, str(CSV))


@pytest.fixture(scope="session")
def tabelas(df_raw):
    return job.normalizar(df_raw)


def _linha(tabela, chave, valor):
    linhas = tabela.where(tabela[chave] == valor).collect()
    assert len(linhas) == 1, f"esperava 1 linha para {chave}={valor}, veio {len(linhas)}"
    return linhas[0]


# ---------------------------------------------------------------------------
# Lógica pura — contagens do dataset real (CA3)
# ---------------------------------------------------------------------------

def test_raw_tem_110_linhas(df_raw):
    assert df_raw.count() == 110


def test_fato_tem_103_linhas(tabelas):
    assert tabelas["fato_pedidos"].count() == 103


def test_fato_tem_24_datas(tabelas):
    assert tabelas["fato_pedidos"].select("data_pedido").distinct().count() == 24


def test_dimensoes_contagem(tabelas):
    assert tabelas["dim_produto"].count() == 8
    assert tabelas["dim_cliente"].count() == 16


def test_fato_uma_linha_por_pedido(tabelas):
    fato = tabelas["fato_pedidos"]
    assert fato.select("pedido_id").distinct().count() == fato.count()


def test_dimensoes_com_chave_unica(tabelas):
    for nome, chave in (("dim_cliente", "cliente_id"), ("dim_produto", "produto_id")):
        dim = tabelas[nome]
        assert dim.select(chave).distinct().count() == dim.count(), nome


def test_sem_fato_orfao(tabelas):
    fato, cli, prod = tabelas["fato_pedidos"], tabelas["dim_cliente"], tabelas["dim_produto"]
    assert fato.join(prod, "produto_id", "left_anti").count() == 0
    assert fato.join(cli, "cliente_id", "left_anti").count() == 0


def test_linhas_invalidas_descartadas(tabelas):
    ids = {r.pedido_id for r in tabelas["fato_pedidos"].select("pedido_id").collect()}
    for invalido in ("PED0096", "PED0097", "PED0099", "PED0100", "PED0101", "PED0106"):
        assert invalido not in ids
    assert None not in ids and "" not in ids


def test_colunas_do_fato(tabelas):
    assert tabelas["fato_pedidos"].columns == [
        "pedido_id", "data_pedido", "cliente_id", "produto_id",
        "preco_unitario", "quantidade", "valor_total",
    ]
    assert tabelas["dim_cliente"].columns == ["cliente_id", "cliente_nome", "cliente_uf"]
    assert tabelas["dim_produto"].columns == ["produto_id", "produto_nome", "categoria"]


def test_valor_total_nao_nulo_e_quantidade_positiva(tabelas):
    fato = tabelas["fato_pedidos"]
    assert fato.where("valor_total IS NULL").count() == 0
    assert fato.where("quantidade IS NULL OR quantidade <= 0").count() == 0


# ---------------------------------------------------------------------------
# Decisões D1–D3
# ---------------------------------------------------------------------------

def test_cliente_sem_nome_vira_desconhecido(tabelas):
    assert _linha(tabelas["dim_cliente"], "cliente_id", "CLI018").cliente_nome == "DESCONHECIDO"


def test_cliente_sem_uf_vira_desconhecido(tabelas):
    assert _linha(tabelas["dim_cliente"], "cliente_id", "CLI019").cliente_uf == "DESCONHECIDO"


def test_chave_conflitante_valor_real_vence(tabelas):
    """PRD072/PRD084 têm atributo vazio numa linha e preenchido em outra (D1)."""
    p72 = _linha(tabelas["dim_produto"], "produto_id", "PRD072")
    p84 = _linha(tabelas["dim_produto"], "produto_id", "PRD084")
    assert (p72.produto_nome, p72.categoria) == ("Smartphone X", "Eletronicos")
    assert (p84.produto_nome, p84.categoria) == ("Mochila Escolar", "Acessorios")


def test_dimensoes_sem_nulos_nem_vazios(tabelas):
    for nome, cols in (("dim_cliente", ["cliente_nome", "cliente_uf"]),
                       ("dim_produto", ["produto_nome", "categoria"])):
        for c in cols:
            assert tabelas[nome].where(f"{c} IS NULL OR trim({c}) = ''").count() == 0, (nome, c)


def test_so_espacos_conta_como_ausente(spark):
    df = spark.createDataFrame(
        [("PED1", "2026-01-01", "C1", "   ", "SP", "P1", "Prod", "Cat", 10.0, 1, 10.0)],
        _schema_texto(),
    )
    assert _linha(job.normalizar(df)["dim_cliente"], "cliente_id", "C1").cliente_nome == "DESCONHECIDO"


def _schema_texto():
    """Schema do raw com a data como string, para poder injetar valores só-espaços."""
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    s = StringType()
    return StructType([
        StructField("pedido_id", s), StructField("data_pedido", s), StructField("cliente_id", s),
        StructField("cliente_nome", s), StructField("cliente_uf", s), StructField("produto_id", s),
        StructField("produto_nome", s), StructField("categoria", s),
        StructField("preco_unitario", DoubleType()), StructField("quantidade", IntegerType()),
        StructField("valor_total", DoubleType()),
    ])


def test_normalizar_e_deterministico(df_raw):
    a = sorted(tuple(r) for r in job.normalizar(df_raw)["dim_produto"].collect())
    b = sorted(tuple(r) for r in job.normalizar(df_raw.orderBy("pedido_id", ascending=False))["dim_produto"].collect())
    assert a == b


# ---------------------------------------------------------------------------
# montar_metadados (RF6)
# ---------------------------------------------------------------------------

def test_montar_metadados_campos_e_tipos():
    item = job.montar_metadados("exec-1", "pedidos", 110, 103, "SUCESSO")
    assert set(item) == {"execution_id", "data_hora", "dataset", "linhas_lidas", "linhas_gravadas", "status"}
    assert isinstance(item["linhas_lidas"], int) and isinstance(item["linhas_gravadas"], int)
    assert (item["linhas_lidas"], item["linhas_gravadas"], item["status"]) == (110, 103, "SUCESSO")
    assert item["dataset"] == "pedidos" and item["execution_id"] == "exec-1"


def test_montar_metadados_data_hora_iso8601():
    from datetime import datetime

    datetime.fromisoformat(job.montar_metadados("e", "d", 1, 1, "FALHA")["data_hora"].replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# I/O: ler_raw / escrever_gold (RF5, RF9, D4, D6)
# ---------------------------------------------------------------------------

def test_escrever_gold_particoes_e_idempotencia(spark, tabelas, tmp_path):
    gold = str(tmp_path / "gold")
    Path(gold).mkdir()
    (Path(gold) / "scripts").mkdir()
    (Path(gold) / "scripts" / "normaliza_pedidos.py").write_text("# script do glue")

    job.escrever_gold(tabelas, gold + "/")
    job.escrever_gold(tabelas, gold + "/")  # 2ª execução: não pode duplicar

    particoes = [p for p in (Path(gold) / "fato_pedidos").iterdir() if p.name.startswith("data_pedido=")]
    assert len(particoes) == 24
    assert spark.read.parquet(gold + "/fato_pedidos").count() == 103
    assert spark.read.parquet(gold + "/dim_cliente").count() == 16
    assert spark.read.parquet(gold + "/dim_produto").count() == 8
    assert not list((Path(gold) / "dim_cliente").glob("data_*="))  # dimensões sem partição
    assert (Path(gold) / "scripts" / "normaliza_pedidos.py").exists()  # D6: não apagou a raiz


# ---------------------------------------------------------------------------
# gravar_metadados_dynamo com boto3 simulado (boto3 tardio)
# ---------------------------------------------------------------------------

def _boto3_falso(monkeypatch, itens):
    class Tabela:
        def __init__(self, nome):
            self.nome = nome

        def put_item(self, Item):
            itens.append((self.nome, Item))

    falso = types.SimpleNamespace(resource=lambda servico, **kw: types.SimpleNamespace(Table=Tabela))
    monkeypatch.setitem(sys.modules, "boto3", falso)


def test_gravar_metadados_dynamo_faz_put_item(monkeypatch):
    itens = []
    _boto3_falso(monkeypatch, itens)
    item = job.montar_metadados("e1", "pedidos", 110, 103, "SUCESSO")
    job.gravar_metadados_dynamo(item, "tabela-x")
    assert itens == [("tabela-x", item)]


def test_modulo_nao_importa_boto3_no_topo():
    assert "import boto3" not in SCRIPT.read_text().split("def gravar_metadados_dynamo")[0]


# ---------------------------------------------------------------------------
# main(): caminho de sucesso e de FALHA com AWS/Glue simulados
# ---------------------------------------------------------------------------

def _simular_glue(monkeypatch, gravados, spark=None, job_name="normaliza-pedidos"):
    args = {"JOB_NAME": job_name, "RAW_PATH": "r", "GOLD_PATH": "g", "DDB_TABLE": "t", "DATASET_NAME": "ds"}
    monkeypatch.setattr(job, "getResolvedOptions", lambda argv, nomes: args)
    monkeypatch.setattr(job, "SparkContext", lambda: types.SimpleNamespace(applicationId="app-1"))
    monkeypatch.setattr(job, "GlueContext", lambda sc: types.SimpleNamespace(spark_session=spark))
    commits = []
    monkeypatch.setattr(
        job, "Job",
        lambda ctx: types.SimpleNamespace(init=lambda *a, **k: None, commit=lambda: commits.append(1)),
    )
    monkeypatch.setattr(job, "gravar_metadados_dynamo", lambda item, tabela: gravados.append((tabela, item)))
    return commits


def test_main_sucesso_grava_sucesso_e_commit(monkeypatch, spark, tmp_path):
    gravados = []
    commits = _simular_glue(monkeypatch, gravados, spark)
    monkeypatch.setattr(job, "ler_raw", lambda s, p: s.read.csv(str(CSV), header=True, schema=job.SCHEMA_RAW))
    monkeypatch.setattr(job, "escrever_gold", lambda tabelas, gold: None)
    job.main()
    (tabela, item), = gravados
    assert tabela == "t"
    assert (item["status"], item["linhas_lidas"], item["linhas_gravadas"]) == ("SUCESSO", 110, 103)
    assert commits == [1]


def test_main_falha_grava_falha_e_repropaga_erro_original(monkeypatch):
    gravados = []
    _simular_glue(monkeypatch, gravados)

    def quebra(spark, path):
        raise RuntimeError("erro original do raw")

    monkeypatch.setattr(job, "ler_raw", quebra)
    with pytest.raises(RuntimeError, match="erro original do raw"):
        job.main()
    (_, item), = gravados
    assert item["status"] == "FALHA" and item["linhas_gravadas"] == 0


def test_main_falha_nao_mascara_erro_se_dynamo_tambem_falhar(monkeypatch):
    gravados = []
    _simular_glue(monkeypatch, gravados)

    def quebra_raw(spark, path):
        raise RuntimeError("erro original do raw")

    def quebra_dynamo(item, tabela):
        raise ConnectionError("dynamo fora do ar")

    monkeypatch.setattr(job, "ler_raw", quebra_raw)
    monkeypatch.setattr(job, "gravar_metadados_dynamo", quebra_dynamo)
    with pytest.raises(RuntimeError, match="erro original do raw"):
        job.main()


def test_execution_id_unico_entre_execucoes(monkeypatch):
    ids = []
    for _ in range(2):
        gravados = []
        _simular_glue(monkeypatch, gravados)
        monkeypatch.setattr(job, "ler_raw", lambda s, p: (_ for _ in ()).throw(RuntimeError("x")))
        with pytest.raises(RuntimeError):
            job.main()
        ids.append(gravados[0][1]["execution_id"])
    assert ids[0] != ids[1]
