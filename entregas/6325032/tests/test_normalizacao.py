"""Testa a implementação do aluno, sem importar o gabarito ou acessar AWS."""
import importlib.util
import json
from datetime import datetime
from pathlib import Path

import pytest
from hypothesis import given, settings, strategies as st
from pyspark.sql import SparkSession, functions as F

RAIZ = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("normaliza_pedidos", RAIZ / "glue-job/normaliza_pedidos.py")
job = importlib.util.module_from_spec(spec)
spec.loader.exec_module(job)


@pytest.fixture(scope="session")
def spark():
    sessao = (SparkSession.builder.master("local[2]").appName("prova-6325032")
              .config("spark.ui.enabled", "false")
              .config("spark.sql.shuffle.partitions", "2")
              .config("spark.sql.session.timeZone", "UTC")
              .getOrCreate())
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()


def linha(pedido="P1", cliente="C1", produto="PR1", quantidade=1,
          nome="Ana", uf="SP", produto_nome="Livro", categoria="Livros", dia="2026-01-05"):
    return (pedido, dia, cliente, nome, uf, produto, produto_nome, categoria,
            10.0, quantidade, None if quantidade is None else 10.0 * quantidade)


def test_dataset_real_contagens_somas_e_integridade(spark):
    raw = job.ler_raw(spark, str(RAIZ / "dataset/pedidos_desnormalizado.csv"))
    tabelas = job.normalizar(raw)
    fato, clientes, produtos = (tabelas[k] for k in ("fato_pedidos", "dim_cliente", "dim_produto"))
    assert raw.count() == 110
    assert fato.count() == 103
    assert clientes.count() == 16
    assert produtos.count() == 8
    assert fato.select("pedido_id").distinct().count() == 103
    assert clientes.select("cliente_id").distinct().count() == 16
    assert produtos.select("produto_id").distinct().count() == 8
    assert fato.select("data_pedido").distinct().count() == 24
    assert fato.agg(F.sum("valor_total")).first()[0] == pytest.approx(61798.90)
    assert fato.join(clientes, "cliente_id", "left_anti").count() == 0
    assert fato.join(produtos, "produto_id", "left_anti").count() == 0
    assert clientes.where("cliente_id = 'CLI018'").first().cliente_nome == "DESCONHECIDO"
    assert clientes.where("cliente_id = 'CLI019'").first().cliente_uf == "DESCONHECIDO"
    assert produtos.where("produto_id = 'PRD084'").first().produto_nome == "Mochila Escolar"
    assert produtos.where("produto_id = 'PRD072'").first().categoria == "Eletronicos"
    receitas = fato.join(produtos, "produto_id").groupBy("categoria").sum("valor_total")
    assert sum(r[1] for r in receitas.collect()) == pytest.approx(61798.90)
    esperado = json.loads((RAIZ / "tests/resultados_esperados.json").read_text())
    for nome, df in tabelas.items():
        df.createOrReplaceTempView(nome)
    # Executa as mesmas quatro consultas da entrega em Spark SQL local.
    # Isso verifica os resultados analíticos, sem afirmar que o Athena já rodou.
    for numero, arquivo in [(1, "01_faturamento_categoria.sql"), (2, "02_top_clientes.sql"),
                            (3, "03_faturamento_dia.sql"), (4, "04_integridade.sql")]:
        resultados = [r.asDict() for r in spark.sql((RAIZ / "sql" / arquivo).read_text()).collect()]
        referencia = esperado[f"consulta_{numero}"]
        if numero == 4:
            referencia = [referencia]
        assert len(resultados) == len(referencia)
        for obtido, alvo in zip(resultados, referencia):
            for chave, valor in alvo.items():
                if chave in {"faturamento", "gasto_total", "faturamento_dia"}:
                    assert obtido[chave] == pytest.approx(float(valor))
                else:
                    assert str(obtido[chave]) == str(valor)


def test_nulos_espacos_quantidades_e_dimensao_desconhecida(spark):
    rows = [linha(), linha(pedido=None), linha(cliente=" "), linha(produto=""),
            linha(quantidade=None), linha(quantidade=0), linha(quantidade=-2),
            linha(pedido="P2", cliente="C2", produto="PR2", nome=" ", uf=None,
                  produto_nome="", categoria=None)]
    tabelas = job.normalizar(spark.createDataFrame(rows, job.SCHEMA_RAW))
    assert {r.pedido_id for r in tabelas["fato_pedidos"].collect()} == {"P1", "P2"}
    assert tabelas["dim_cliente"].where("cliente_id='C2'").first().asDict() == {
        "cliente_id": "C2", "cliente_nome": "DESCONHECIDO", "cliente_uf": "DESCONHECIDO"}
    assert tabelas["dim_produto"].where("produto_id='PR2'").first().asDict() == {
        "produto_id": "PR2", "produto_nome": "DESCONHECIDO", "categoria": "DESCONHECIDO"}


def test_deduplicacao_estavel_e_cadastro_completo(spark):
    rows = [linha(nome=None, categoria=None), linha(), linha(pedido="P2", nome="Zelia")]
    a = job.normalizar(spark.createDataFrame(rows, job.SCHEMA_RAW).repartition(2))
    b = job.normalizar(spark.createDataFrame(list(reversed(rows)), job.SCHEMA_RAW).repartition(1))
    for nome in a:
        assert {tuple(r) for r in a[nome].collect()} == {tuple(r) for r in b[nome].collect()}
    assert a["fato_pedidos"].count() == 2
    assert a["dim_cliente"].first().cliente_nome == "Ana"
    assert a["dim_produto"].first().categoria == "Livros"


def test_entrada_vazia(spark):
    tabelas = job.normalizar(spark.createDataFrame([], job.SCHEMA_RAW))
    assert all(df.count() == 0 for df in tabelas.values())


def test_parquet_roundtrip_particoes_e_reexecucao(spark, tmp_path):
    raw = job.ler_raw(spark, str(RAIZ / "dataset/pedidos_desnormalizado.csv"))
    tabelas = job.normalizar(raw)
    job.escrever_gold(tabelas, str(tmp_path))
    assert len(list((tmp_path / "fato_pedidos").glob("data_pedido=*"))) == 24
    fato = spark.read.parquet(str(tmp_path / "fato_pedidos"))
    assert fato.count() == 103
    assert fato.agg(F.sum("valor_total")).first()[0] == pytest.approx(61798.90)
    for nome in ("dim_cliente", "dim_produto"):
        assert not list((tmp_path / nome).glob("*=*"))
        assert spark.read.parquet(str(tmp_path / nome)).count() == tabelas[nome].count()
    # Um segundo snapshot menor também remove partições antigas.
    menor = job.normalizar(raw.where(F.col("data_pedido") == "2026-01-05"))
    job.escrever_gold(menor, str(tmp_path))
    assert len(list((tmp_path / "fato_pedidos").glob("data_pedido=*"))) == 1
    assert spark.read.parquet(str(tmp_path / "fato_pedidos")).count() == menor["fato_pedidos"].count()


def test_metadados_timezone_tipos_e_validacoes():
    item = job.montar_metadados("run-1", "pedidos", 110, 103, "SUCESSO")
    assert set(item) == {"execution_id", "data_hora", "dataset", "linhas_lidas", "linhas_gravadas", "status"}
    assert datetime.fromisoformat(item["data_hora"].replace("Z", "+00:00")).utcoffset().total_seconds() == 0
    assert type(item["linhas_gravadas"]) is int
    for lidas, gravadas, status in [(-1, 0, "FALHA"), (1, 2, "SUCESSO"), (1, 1, "OK")]:
        with pytest.raises(ValueError):
            job.montar_metadados("id", "dataset", lidas, gravadas, status)


def test_dynamo_sem_acessar_aws(monkeypatch):
    import boto3
    chamadas = []
    class Tabela:
        def put_item(self, **kwargs):
            chamadas.append(kwargs["Item"])
    class Recurso:
        def Table(self, nome):
            assert nome == "execucoes"
            return Tabela()
    def resource(servico, region_name):
        assert (servico, region_name) == ("dynamodb", "us-east-1")
        return Recurso()
    monkeypatch.setattr(boto3, "resource", resource)
    item = job.montar_metadados("id", "pedidos", 110, 103, "SUCESSO")
    job.gravar_metadados_dynamo(item, "execucoes")
    assert chamadas == [item]


@pytest.mark.parametrize("ddb_falha", [False, True])
def test_falha_preserva_contagens_e_excecao_original(spark, monkeypatch, ddb_falha):
    import sys
    from types import ModuleType
    from unittest.mock import MagicMock
    from pyspark.context import SparkContext

    args = dict(JOB_NAME="teste", RAW_PATH="raw", GOLD_PATH="gold", DDB_TABLE="execucoes", DATASET_NAME="pedidos")
    contexto = MagicMock()
    for nome, atributo, valor in [
        ("awsglue.context", "GlueContext", lambda sc: contexto),
        ("awsglue.job", "Job", MagicMock()),
        ("awsglue.utils", "getResolvedOptions", lambda argv, nomes: args),
    ]:
        modulo = ModuleType(nome)
        setattr(modulo, atributo, valor)
        monkeypatch.setitem(sys.modules, nome, modulo)
    monkeypatch.setattr(SparkContext, "getOrCreate", lambda: object())
    raw = MagicMock()
    raw.cache.return_value = raw
    raw.count.return_value = 110
    fato = MagicMock()
    fato.cache.return_value = fato
    fato.where.return_value.limit.return_value.count.return_value = 0
    monkeypatch.setattr(job, "ler_raw", lambda *args: raw)
    monkeypatch.setattr(job, "normalizar", lambda df: {"fato_pedidos": fato})
    erro_original = RuntimeError("Falha na escrita do Parquet")
    def escrita(*args):
        raise erro_original
    monkeypatch.setattr(job, "escrever_gold", escrita)
    itens = []
    def registrar(item, tabela):
        itens.append(item)
        if ddb_falha:
            raise RuntimeError("DynamoDB indisponível")
    monkeypatch.setattr(job, "gravar_metadados_dynamo", registrar)
    with pytest.raises(RuntimeError) as erro:
        job.main()
    assert erro.value is erro_original
    assert itens[0]["status"] == "FALHA"
    assert itens[0]["linhas_lidas"] == 110
    assert itens[0]["linhas_gravadas"] == 0


# 100 entradas geradas: compara o fato com um oráculo Python independente.
@settings(max_examples=100, deadline=None)
@given(st.lists(st.tuples(st.booleans(), st.booleans(), st.booleans(),
                          st.one_of(st.none(), st.integers(-2, 5))), min_size=0, max_size=12))
def test_propriedade_descarte(spark, dados):
    rows, esperados = [], set()
    for i, (tem_pedido, tem_cliente, tem_produto, quantidade) in enumerate(dados):
        pedido = f"P{i}" if tem_pedido else None
        cliente = f"C{i}" if tem_cliente else " "
        produto = f"PR{i}" if tem_produto else ""
        rows.append(linha(pedido, cliente, produto, quantidade))
        if tem_pedido and tem_cliente and tem_produto and quantidade is not None and quantidade > 0:
            esperados.add(pedido)
    fato = job.normalizar(spark.createDataFrame(rows, job.SCHEMA_RAW))["fato_pedidos"]
    assert {r.pedido_id for r in fato.collect()} == esperados
