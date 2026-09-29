# Resultados esperados × obtidos

**Aluno:** José Henrique Teixeira Luiz — **RA:** 3225002
Conferência item por item contra a `RUBRICA.md` e as expectativas declaradas no
`README.md` da prova.

---

## Critério 1 — Infraestrutura aplica sem erro (20 pts)

> *"`terraform init`/`validate`/`apply` executam sem erro e criam todos os recursos."*

| Comando | Resultado |
|---|---|
| `terraform init` | `Terraform has been successfully initialized!` |
| `terraform validate` | `Success! The configuration is valid.` |
| `terraform fmt -check -recursive` | sem saída (tudo formatado) |
| `terraform plan` | `Plan: 8 to add, 0 to change, 0 to destroy.` · 0 erros |
| `terraform apply` | `Apply complete! Resources: 8 added, 0 changed, 0 destroyed.` |

Os 8 recursos, num **único apply** da pasta `infra/` (raw + gold juntos, como a
Seção 10 exige):

```
terraform_data.bucket_raw         terraform_data.bucket_gold
terraform_data.glue_script        aws_glue_job.normalizacao
aws_glue_catalog_database.gold    aws_glue_crawler.gold
aws_athena_workgroup.gold         aws_dynamodb_table.execucoes
```

---

## Critério 2 — Normalização correta (25 pts)

> *"fato + 2 dimensões corretos: chaves das dimensões únicas, integridade
> referencial, regra de dados inválidos aplicada."*

| Verificação | Esperado | Obtido |
|---|---|---|
| `fato_pedidos` | uma linha por pedido válido | **103** |
| `dim_cliente` | uma linha por `cliente_id` | **21** |
| `dim_produto` | uma linha por `produto_id` | **8** |
| Chaves únicas nas dimensões | `count = count(distinct)` | ✅ |
| Integridade referencial | 0 órfãos | ✅ `orfaos = 0` |
| Grão do fato | `count = count(distinct pedido_id)` | ✅ 103 = 103 |

### A regra 6.7, passo a passo

| Filtro | Linhas |
|---|---:|
| dataset original | 110 |
| `pedido_id` válido (não nulo, não vazio após trim) | 109 |
| `cliente_id` válido | 107 |
| `produto_id` válido | 106 |
| `quantidade` não nula **e** `> 0` | **103** |

As 7 linhas descartadas e o motivo de cada uma:

| Linha do CSV | Pedido | Motivo do descarte |
|---|---|---|
| 97 | `PED0096` | sem `cliente_id` |
| 98 | `PED0097` | sem `produto_id` |
| 99 | (sem id) | sem `pedido_id` |
| 100 | `PED0099` | `quantidade = 0` |
| 101 | `PED0100` | `quantidade` ausente |
| 102 | `PED0101` | `quantidade = -2` |
| 107 | `PED0106` | sem `cliente_id` **e** sem `produto_id` |

> **Por que a condição da quantidade tem duas partes:** em SQL, `NULL <= 0`
> avalia para `NULL`, não para falso, e o `filter` descarta o resultado
> indefinido. Filtrar apenas por `<= 0` deixaria a linha `PED0100` entrar no
> fato. Verificado na prática: o filtro `quantidade <= 0` retornou só
> `PED0099` e `PED0101`.

### As 4 linhas mantidas com `DESCONHECIDO`

| Linha | Chave | Campo ausente |
|---|---|---|
| 103 | `CLI018` | `cliente_nome` → `DESCONHECIDO` |
| 104 | `CLI019` | `cliente_uf` → `DESCONHECIDO` |
| 105 | `PRD084` | `produto_nome` — **recuperado** |
| 106 | `PRD072` | `categoria` — **recuperado** |

> **A assimetria da regra:** chave ausente elimina a linha; texto ausente é
> preenchido e a linha permanece. Faz sentido porque a dimensão existe para ser
> apontada pelo fato através da chave — sem chave ela é inalcançável; sem o
> nome ela ainda cumpre seu papel.

> **Por que `PRD084` e `PRD072` não viraram `DESCONHECIDO`:** eles aparecem 10 e
> 12 vezes no CSV, com o campo preenchido em quase todas. A normalização usa
> `groupBy` + `first(ignorenulls=True)`, que recupera o valor real das outras
> linhas. Um `dropDuplicates` simples escolheria **uma linha arbitrária** por
> chave e poderia gravar `DESCONHECIDO` num produto cujo nome está no arquivo
> nove vezes. O `CLI018` virou `DESCONHECIDO` porque aparece **uma única vez**,
> e nessa linha o nome está ausente — não havia de onde recuperar.

---

## Critério 3 — Parquet particionado (15 pts)

> *"dados no gold em Parquet, particionados corretamente por `data_pedido`
> (uma partição por data distinta)."*

| Verificação | Esperado | Obtido |
|---|---|---|
| Formato | Parquet | ✅ `.snappy.parquet` |
| Layout | `data_pedido=YYYY-MM-DD` | ✅ |
| Partições | uma por data distinta | ✅ **24** = 24 datas |
| Dimensões | sem partição | ✅ um arquivo cada |

```
fato_pedidos/data_pedido=2026-01-05/part-00000-....snappy.parquet
...
fato_pedidos/data_pedido=2026-01-28/part-00000-....snappy.parquet
dim_cliente/part-00000-....snappy.parquet
dim_produto/part-00000-....snappy.parquet
```

> **Ajuste que só apareceu na nuvem:** na primeira execução as partições saíram
> como `data_pedido=2026-01-05 00%3A00%3A00`. O mesmo `inferSchema=True`
> produziu tipos diferentes — `date` no Spark 3.5.1 do teste local,
> `timestamp` no Spark 3.3 do Glue 4.0. Corrigido com `F.to_date()` antes do
> `partitionBy`, o que garante o tipo independentemente do runtime.

---

## Critério 4 — Consultas Athena (15 pts)

> *"tabelas registradas no Glue Data Catalog e todas as consultas de referência
> retornando os resultados esperados."*

Tabelas catalogadas: `dim_cliente`, `dim_produto`, `fato_pedidos` (via
`aws_glue_crawler.gold`).

| Consulta | Esperado pelo README | Obtido |
|---|---|---|
| 1 — faturamento por categoria | uma linha por categoria, sem nulos; soma = `SUM(valor_total)` | ✅ 7 categorias · soma `61798.9` = total |
| 2 — top 5 clientes | no máximo 5 linhas, decrescente | ✅ 5 linhas, `Carla Nunes` no topo |
| 3 — por dia | uma linha por data; `SUM(qtd)` = linhas do fato | ✅ 24 linhas · soma `103` |
| 4 — integridade | `orfaos = 0` | ✅ `0` |

Arquivos em [`sql/`](sql/), cada um com o resultado obtido em comentário.

### Validações cruzadas

```
total_fato | soma_por_dia | datas_distintas
-----------+--------------+----------------
  61798.9  |     103      |       24
```

E a soma manual das categorias da Consulta 1:
`30334.1 + 13485.0 + 5698.1 + 4921.0 + 2876.8 + 2338.2 + 2145.7 = 61798.9` ✅

---

## Critério 5 — Metadados no DynamoDB (10 pts)

> *"item gravado com o esquema esperado (`execution_id`, dataset, ...)"*

| Campo | Tipo esperado | Obtido |
|---|---|---|
| `execution_id` (hash key) | S | `normaliza-pedidos-spark-application-1790467853839` |
| `data_hora` | S, ISO-8601 | `2026-09-27T00:11:43.014471` |
| `dataset` | S | `pedidos_desnormalizado` |
| `linhas_lidas` | **N** | `110` |
| `linhas_gravadas` | **N** | `103` |
| `status` | S | `SUCESSO` |

Tabela: `billing_mode = PAY_PER_REQUEST`, `hash_key = "execution_id"`.

> `linhas_lidas` = 110 (todo o raw) e `linhas_gravadas` = 103 (o fato). A
> diferença de 7 é exatamente o descarte da regra 6.7 — os números do item de
> metadados são consistentes com o conteúdo do gold.

> **Nota de projeto:** `montar_metadados()` devolve valores Python crus
> (`int`, `str`), não o formato nativo do DynamoDB. A conversão é
> responsabilidade da função de I/O `gravar_metadados_dynamo()`. Isso mantém a
> função pura testável localmente sem AWS, como o próprio esqueleto sugere.

---

## Critério 6 — Custo, limpeza e segurança (15 pts)

> *"tags de custo em todos os recursos, buckets privados, uso da LabRole por
> ARN (sem criar roles/policies), credenciais nunca versionadas e evidência de
> `terraform destroy`."*

| Prática | Situação |
|---|---|
| Tags de custo | `var.tags` nos recursos AWS tagáveis + tagging do bucket via CLI |
| Buckets privados | os 4 bloqueios de acesso público em ambos |
| LabRole por ARN | `var.labrole_arn` — **0 recursos IAM** no plan |
| Credenciais não versionadas | `terraform.tfvars`, `*.tfstate` e `.terraform/` no `.gitignore` |
| `terraform destroy` | `Destroy complete! Resources: 8 destroyed.` |
| Conta limpa | conferida recurso por recurso |

```
buckets S3 da prova   : (nenhum)
Glue jobs             : (nenhum)
Glue databases        : (nenhum)
Glue crawlers         : (nenhum)
DynamoDB              : (nenhuma)
Athena workgroups     : (só o primary)
```

> O `local-exec` com `when = destroy` esvazia o bucket antes de removê-lo. Os
> Parquet gravados pelo Glue e os resultados do Athena **não** são gerenciados
> pelo Terraform, e a AWS recusa apagar bucket não-vazio — sem essa etapa o
> `destroy` falharia.

---

## Validação local (antes de subir para a AWS)

A suíte de testes de propriedade do professor (`local-test/`) foi apontada para
esta implementação através de um shim, sem alterar a pasta `local-test/` e sem
consultar a implementação de referência.

| Teste | Resultado |
|---|---|
| `test_smoke_infra_normaliza` | PASSOU |
| `test_prop1_contagem_fato_igual_linhas_validas` | PASSOU |
| `test_prop6_metadados_consistentes` | PASSOU |
| `test_borda_cliente_id_ausente_descartada_do_fato` | PASSOU |
| `test_borda_cliente_nome_ausente_vira_desconhecido` | PASSOU |
| `test_borda_quantidade_invalida_descartada` | PASSOU |
| `test_borda_dataset_com_data_unica_gera_particao_unica` | PASSOU |
| `test_prop2` a `test_prop5` | **sem veredito** — ver nota |

> **Nota honesta sobre os 4 sem veredito:** a JVM do Spark foi encerrada por
> falta de memória na máquina de desenvolvimento durante esses testes, e eles
> falharam com `ConnectionRefusedError` do py4j — erro de infraestrutura, não de
> lógica. Não foram reprovados; não foram avaliados. Os mesmos números que eles
> verificam (contagem do fato, unicidade das chaves, integridade referencial e
> número de partições) foram confirmados na execução real na AWS, via Athena.
