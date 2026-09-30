"""Verificações do script efetivamente entregue, sem importar o gabarito."""
import importlib.util
from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

BASE = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("normaliza_pedidos_aluno", BASE / "glue-job/normaliza_pedidos.py")
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)


@pytest.fixture(scope="module")
def spark():
    sessao = (SparkSession.builder.master("local[2]").appName("teste-entrega-6325231")
              .config("spark.sql.shuffle.partitions", "2")
              .config("spark.ui.enabled", "false").getOrCreate())
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()


def _linha(pedido="1", cliente="C1", produto="P1", quantidade=2, nome="Ana",
           categoria="Livro", data=date(2026, 1, 5)):
    return (pedido, data, cliente, nome, "SP", produto, "Livro", categoria, 10.0, quantidade, 20.0)


def test_regras_e_conflitos_deterministicos(spark):
    linhas = [
        _linha(), _linha(pedido="2", cliente=None), _linha(pedido="3", produto=" "),
        _linha(pedido="4", quantidade=None), _linha(pedido="5", quantidade=0),
        _linha(pedido="6", quantidade=-1), _linha(pedido="7", cliente="C2", nome=" "),
        _linha(pedido="8", produto="P2", categoria=""),
        _linha(pedido="9", cliente="C3", nome=None, quantidade=0),
        _linha(pedido="10", produto="P3", categoria=None, quantidade=0),
        _linha(pedido="1", cliente="C1", nome="Ana"),
    ]
    bruto = spark.createDataFrame(linhas, mod.RAW_SCHEMA)
    tabelas = mod.normalizar(bruto)
    fato = tabelas["fato_pedidos"]
    assert {r.pedido_id for r in fato.select("pedido_id").collect()} == {"1", "7", "8"}
    assert fato.count() == fato.select("pedido_id").distinct().count()
    clientes = {r.cliente_id: r.cliente_nome for r in tabelas["dim_cliente"].collect()}
    produtos = {r.produto_id: r.categoria for r in tabelas["dim_produto"].collect()}
    assert clientes["C2"] == clientes["C3"] == "DESCONHECIDO"
    assert produtos["P2"] == produtos["P3"] == "DESCONHECIDO"
    assert clientes.keys() >= {"C1", "C2", "C3"}
    assert produtos.keys() >= {"P1", "P2", "P3"}
    assert fato.select("cliente_id").distinct().subtract(tabelas["dim_cliente"].select("cliente_id")).count() == 0
    assert fato.select("produto_id").distinct().subtract(tabelas["dim_produto"].select("produto_id")).count() == 0
    assert fato.schema["data_pedido"].dataType.simpleString() == "date"
    assert fato.schema["quantidade"].dataType.simpleString() == "int"


def test_dataset_real_parquet_reexecucao_e_metadados(spark, tmp_path, monkeypatch):
    bruto = mod.ler_raw(spark, str(BASE / "dataset/pedidos_desnormalizado.csv"))
    tabelas = mod.normalizar(bruto)
    fato = tabelas["fato_pedidos"]
    assert bruto.count() == 110
    assert fato.count() == 103
    assert fato.select("pedido_id").distinct().count() == 103
    assert fato.select("cliente_id").distinct().subtract(tabelas["dim_cliente"].select("cliente_id")).count() == 0
    assert fato.select("produto_id").distinct().subtract(tabelas["dim_produto"].select("produto_id")).count() == 0
    assert fato.where("abs(valor_total - preco_unitario * quantidade) > 0.011").count() == 0
    destino = str(tmp_path / "gold")
    mod.escrever_gold(tabelas, destino)
    particoes = {p.name for p in (tmp_path / "gold/fato_pedidos").iterdir() if p.is_dir()}
    assert len(particoes) == 24
    assert all(p.startswith("data_pedido=2026-") for p in particoes)
    assert spark.read.parquet(destino + "/fato_pedidos").count() == 103
    mod.escrever_gold(tabelas, destino)
    assert spark.read.parquet(destino + "/fato_pedidos").count() == 103
    assert spark.read.parquet(destino + "/dim_cliente").count() == tabelas["dim_cliente"].count()
    assert spark.read.parquet(destino + "/dim_produto").count() == tabelas["dim_produto"].count()
    item = mod.montar_metadados("id-1", "pedidos_desnormalizado", 110, 103, "SUCESSO")
    assert item["status"] == "SUCESSO" and item["linhas_lidas"] == 110 and item["linhas_gravadas"] == 103
    assert item["data_hora"].endswith("+00:00")
    fake_table = Mock()
    fake_resource = Mock()
    fake_resource.Table.return_value = fake_table
    import sys, types
    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(resource=Mock(return_value=fake_resource)))
    mod.gravar_metadados_dynamo(item, "execucoes")
    fake_table.put_item.assert_called_once_with(Item=item)


def test_falha_de_metadados_e_erro_original(monkeypatch, spark):
    class Context:
        spark_session = object()
    class Job:
        def __init__(self, _): pass
        def init(self, *_): pass
        def commit(self): pass
    monkeypatch.setattr(mod, "getResolvedOptions", lambda *_: {
        "JOB_NAME": "teste", "RAW_PATH": "raw", "GOLD_PATH": "gold",
        "DDB_TABLE": "execucoes", "DATASET_NAME": "pedidos"
    })
    monkeypatch.setattr(mod, "SparkContext", Mock(getOrCreate=Mock(return_value=object())))
    monkeypatch.setattr(mod, "GlueContext", lambda _: Context())
    monkeypatch.setattr(mod, "Job", Job)
    monkeypatch.setattr(mod, "ler_raw", lambda *_: Mock(count=Mock(return_value=2)))
    # Erro ocorre antes da escrita; o registro FALHA deve preservar a exceção original.
    monkeypatch.setattr(mod, "escrever_gold", Mock())
    fake_fato = Mock()
    fake_fato.count.return_value = 1
    fake_fato.where.return_value.limit.return_value.count.return_value = 0
    monkeypatch.setattr(mod, "normalizar", lambda *_: {"fato_pedidos": fake_fato})
    erro = RuntimeError("falha de escrita")
    mod.escrever_gold.side_effect = erro
    registros = []
    monkeypatch.setattr(mod, "gravar_metadados_dynamo", lambda item, _: registros.append(item))
    with pytest.raises(RuntimeError, match="falha de escrita"):
        mod.main()
    assert len(registros) == 1 and registros[0]["status"] == "FALHA"
    assert registros[0]["linhas_gravadas"] == 0
