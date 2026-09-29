# Bancada de testes local

Como eu validei a normalização **antes** de subir qualquer coisa para a AWS.

## O problema

A pasta `local-test/` do repositório da prova traz uma suíte de 11 testes — 6
propriedades com Hypothesis (100+ iterações cada) e 5 casos de borda. Só que
ela importa de `normalizacao_referencia`, que é a **implementação de referência
do professor**:

```python
from normalizacao_referencia import montar_metadados, normalizar
```

Ou seja: rodando como está, a suíte testa o gabarito, não o meu código.

## A solução: um shim

Esta bancada não copia o gabarito. No lugar dele entra um módulo de **mesmo
nome** que apenas reexporta as funções do meu `normaliza_pedidos.py`:

```python
# normalizacao_referencia.py  —  NAO e o gabarito
from normaliza_pedidos import montar_metadados, normalizar
```

Com isso os 11 testes do professor passam a avaliar a **minha** implementação,
sem alterar nada em `local-test/` e sem abrir a implementação de referência.

## Os arquivos

| Arquivo | Papel |
|---|---|
| `Dockerfile` | Java 21 + PySpark 3.5.1, igual ao do professor, mas copiando o meu código |
| `normalizacao_referencia.py` | o shim de 1 linha descrito acima |
| `requirements.txt` | `pyspark==3.5.1`, `pytest`, `hypothesis` |
| `testar.sh` | roda a suíte inteira num container |
| `testar-um-por-um.sh` | **um container por teste**, com a JVM limitada |
| `conferir.sh` | em 1 s, sem Docker, diz quais das 5 funções já estão implementadas |

## Por que existe o `testar-um-por-um.sh`

Rodar os 11 testes num processo só estourava a memória da VM do Docker e a JVM
do Spark era encerrada no meio. A partir daí **todos** os testes seguintes
falhavam com `ConnectionRefusedError` do py4j — um erro de infraestrutura que
não diz nada sobre o código, mas que à primeira vista parece catástrofe.

Um container por teste resolve: cada um começa com a JVM limpa e morre em
seguida.

```bash
./testar.sh -k borda        # só os testes de borda, ~18 s
./testar-um-por-um.sh       # a suíte inteira, um container por teste
./conferir.sh               # estado das 5 funções, instantâneo
```

## O `conferir.sh` nasceu de um erro meu

Duas vezes eu editei a cópia descartável do arquivo em vez do arquivo que vale,
e perdi o trabalho. O `conferir.sh` lê o arquivo **da entrega** e diz em um
segundo o que já está implementado. Virou regra: **salvar → `conferir.sh` →
testar**.

## Resultado

Ver [`../evidencias/pytest-local.txt`](../evidencias/pytest-local.txt).
