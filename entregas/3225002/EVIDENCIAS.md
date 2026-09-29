# Evidências de Execução — Prova Prática de Big Data na AWS

**Aluno:** José Henrique Teixeira Luiz
**RA:** 3225002
**Conta AWS Academy Learner Lab:** `447916381827` · região `us-east-1`
**Data da execução:** 26–27/09/2026

Pipeline executado de ponta a ponta: **S3 raw (CSV) → Glue Job PySpark → S3 gold
(Parquet particionado) → Athena (SQL)**, com metadados da execução no DynamoDB.

> 📸 **Capturas de tela** do console AWS e do terminal em
> [`evidencias/`](evidencias/) — ver o [índice](evidencias/README.md).
> O pipeline foi executado duas vezes, em contas diferentes do Learner Lab
> (`447916381827` em 26–27/09 e `591657390055` em 29/09), com resultados
> idênticos. Os outputs em texto abaixo são da primeira execução; as capturas,
> da segunda.

---

## 1. `terraform apply` — 8 recursos

Um único `apply` na pasta `infra/` criou as camadas raw e gold, como o
enunciado exige.

```
$ terraform plan
Plan: 8 to add, 0 to change, 0 to destroy.

  aws_athena_workgroup.gold        will be created
  aws_dynamodb_table.execucoes     will be created
  aws_glue_catalog_database.gold   will be created
  aws_glue_crawler.gold            will be created
  aws_glue_job.normalizacao        will be created
  terraform_data.bucket_gold       will be created
  terraform_data.bucket_raw        will be created
  terraform_data.glue_script       will be created
```

```
$ terraform apply

terraform_data.bucket_raw  (local-exec): upload: ../dataset/pedidos_desnormalizado.csv
                                         to s3://prova-bigdata-raw-3225002/pedidos/pedidos_desnormalizado.csv
terraform_data.bucket_gold (local-exec): "BucketArn": "arn:aws:s3:::prova-bigdata-gold-3225002"
terraform_data.glue_script (local-exec): upload: ../glue-job/normaliza_pedidos.py
                                         to s3://prova-bigdata-gold-3225002/jobs/normaliza_pedidos.py
aws_athena_workgroup.gold:      Creation complete after 1s [id=prova-bigdata-gold]
aws_glue_catalog_database.gold: Creation complete after 1s [id=447916381827:prova_bigdata_gold]
aws_dynamodb_table.execucoes:   Creation complete after 8s [id=execucoes]
aws_glue_job.normalizacao:      Creation complete after 1s [id=normaliza-pedidos]
aws_glue_crawler.gold:          Creation complete after 0s [id=cataloga-gold-pedidos]

Apply complete! Resources: 8 added, 0 changed, 0 destroyed.
```

### Conferência de segurança no plan

```
IAM a criar                 : 0     (LabRole referenciada por ARN, nunca criada)
DynamoDB billing_mode       : PAY_PER_REQUEST
DynamoDB hash_key           : execution_id
Buckets                     : criados via AWS CLI (terraform_data + local-exec)
Bloqueio de acesso público  : os 4 flags aplicados em ambos os buckets
```

> **Por que os buckets não usam `aws_s3_bucket`:** a SCP da organização do
> Learner Lab nega `s3:GetBucketObjectLockConfiguration`, que o recurso
> `aws_s3_bucket` sempre lê, resultando em `AccessDenied`. Segui o padrão do
> `raw.tf` fornecido pelo professor: criação via AWS CLI dentro do próprio
> `apply`.

---

## 2. Glue Job — `SUCCEEDED`

```
$ aws glue start-job-run --job-name normaliza-pedidos
JobRunId: jr_2cac01425a22b82fa75de22f3a628dd43e5cd9138f42aee0a7a089f35fe00d1d

$ aws glue get-job-run --job-name normaliza-pedidos --run-id jr_2cac...
{
    "Estado": "SUCCEEDED",
    "Erro": null,
    "Tempo": 87
}
```

Argumentos passados ao Job:

```
--RAW_PATH      s3://prova-bigdata-raw-3225002/pedidos/
--GOLD_PATH     s3://prova-bigdata-gold-3225002/
--DDB_TABLE     execucoes
--DATASET_NAME  pedidos_desnormalizado
```

---

## 3. Camada gold — Parquet particionado por `data_pedido`

```
$ aws s3 ls s3://prova-bigdata-gold-3225002/ --recursive

  dim_cliente/part-00000-7e77bbd9-...-c000.snappy.parquet
  dim_produto/part-00000-47eccd50-...-c000.snappy.parquet
  fato_pedidos/data_pedido=2026-01-05/part-00000-...snappy.parquet
  fato_pedidos/data_pedido=2026-01-06/part-00000-...snappy.parquet
  ...
  fato_pedidos/data_pedido=2026-01-28/part-00000-...snappy.parquet
```

```
$ aws s3 ls s3://prova-bigdata-gold-3225002/fato_pedidos/ | grep -c 'data_pedido='
24
```

**24 partições — uma por data distinta**, no formato `data_pedido=YYYY-MM-DD`
exigido pela Seção 6 do README. As dimensões foram gravadas sem partição.

### Ajuste necessário: inferência de schema difere entre Spark local e Glue

Na primeira execução as partições saíram assim:

```
fato_pedidos/data_pedido=2026-01-05 00%3A00%3A00/
```

O `%3A` é `:` codificado — a coluna tinha componente de hora. Causa:

| Ambiente | Versão do Spark | Tipo inferido para `data_pedido` |
|---|---|---|
| Container local de teste | 3.5.1 | `date` |
| AWS Glue 4.0 | 3.3 | `timestamp` |

O mesmo `inferSchema=True` produziu tipos diferentes. Corrigido com
`F.to_date()` antes do `partitionBy`, garantindo o tipo independentemente do
runtime. O `terraform_data.glue_script` usa
`triggers_replace = [filesha256(...)]`, então o novo script subiu sozinho no
`apply` seguinte, sem recriar nenhum outro recurso.

---

## 4. Glue Data Catalog

```
$ aws glue get-tables --database-name prova_bigdata_gold --query 'TableList[].Name'
dim_cliente     dim_produto     fato_pedidos
```

Schema catalogado de `fato_pedidos`:

```
pedido_id       string
cliente_id      string
produto_id      string
preco_unitario  double
quantidade      int
valor_total     double
--- chave de partição ---
data_pedido     string
```

---

## 5. Consultas Athena de referência (Seção 8)

Workgroup: `prova-bigdata-gold` · Database: `prova_bigdata_gold`

### Consulta 1 — Faturamento por categoria

```sql
SELECT p.categoria, ROUND(SUM(f.valor_total), 2) AS faturamento
FROM fato_pedidos f
JOIN dim_produto p ON f.produto_id = p.produto_id
GROUP BY p.categoria
ORDER BY faturamento DESC;
```

```
categoria         | faturamento
------------------+------------
Eletronicos       |   30334.1
Moveis            |   13485.0
Calcados          |    5698.1
Eletrodomesticos  |    4921.0
Livros            |    2876.8
Acessorios        |    2338.2
Vestuario         |    2145.7
```

✅ Uma linha por categoria, **sem nulos e sem `DESCONHECIDO`**.
✅ A soma das linhas (`61798.9`) é igual a `SUM(valor_total)` do fato.

### Consulta 2 — Top 5 clientes por gasto

```sql
SELECT c.cliente_nome, ROUND(SUM(f.valor_total), 2) AS gasto_total
FROM fato_pedidos f
JOIN dim_cliente c ON f.cliente_id = c.cliente_id
GROUP BY c.cliente_nome
ORDER BY gasto_total DESC
LIMIT 5;
```

```
cliente_nome   | gasto_total
---------------+------------
Carla Nunes    |     6955.2
Diego Alves    |     6464.7
Henrique Melo  |     6114.4
Ana Souza      |     5793.3
Elaine Rocha   |     5284.3
```

✅ 5 linhas, ordenadas de forma decrescente.

### Consulta 3 — Pedidos e faturamento por dia (partição)

```sql
SELECT data_pedido, COUNT(*) AS qtd_pedidos, ROUND(SUM(valor_total), 2) AS faturamento_dia
FROM fato_pedidos
GROUP BY data_pedido
ORDER BY data_pedido;
```

```
data_pedido | qtd_pedidos | faturamento_dia
------------+-------------+----------------
2026-01-05  |      5      |     1499.0
2026-01-06  |      4      |      948.1
2026-01-07  |      4      |     1798.3
2026-01-08  |      4      |     2947.7
2026-01-09  |      4      |     1167.1
2026-01-10  |      4      |     3106.7
   ...        (24 linhas no total)
```

✅ Uma linha por data distinta (24), coincidindo com as partições do gold.
✅ `SUM(qtd_pedidos)` = 103 = total de linhas de `fato_pedidos`.

### Consulta 4 — Integridade referencial

```sql
SELECT COUNT(*) AS orfaos
FROM fato_pedidos f
LEFT JOIN dim_produto p ON f.produto_id = p.produto_id
WHERE p.produto_id IS NULL;
```

```
orfaos
------
  0
```

✅ Nenhum órfão: toda FK do fato existe na dimensão.

### Validações cruzadas exigidas pelo README

```sql
SELECT (SELECT ROUND(SUM(valor_total),2) FROM fato_pedidos)                            AS total_fato,
       (SELECT SUM(qtd) FROM (SELECT COUNT(*) AS qtd FROM fato_pedidos GROUP BY data_pedido)) AS soma_por_dia,
       (SELECT COUNT(DISTINCT data_pedido) FROM fato_pedidos)                          AS datas_distintas;
```

```
total_fato | soma_por_dia | datas_distintas
-----------+--------------+----------------
  61798.9  |     103      |       24
```

---

## 6. Modelo dimensional — contagens

| Tabela | Linhas | Observação |
|---|---:|---|
| `fato_pedidos` | **103** | 110 do raw menos 7 inválidas |
| `dim_cliente` | **21** | uma linha por `cliente_id` |
| `dim_produto` | **8** | uma linha por `produto_id` |

### Como se chega em 103 (regra 6.7)

| Filtro aplicado | Linhas restantes |
|---|---:|
| dataset original | 110 |
| `pedido_id` válido | 109 |
| `cliente_id` válido | 107 |
| `produto_id` válido | 106 |
| `quantidade` não nula **e** > 0 | **103** |

E `COUNT(DISTINCT pedido_id)` = 103, confirmando o grão de uma linha por pedido.

> **Detalhe da regra:** a condição da quantidade precisa das **duas** partes.
> Em SQL, `NULL <= 0` avalia para `NULL`, não para falso — filtrar apenas por
> `<= 0` deixaria passar a linha `PED0100`, que tem a quantidade ausente.

---

## 7. Item de metadados no DynamoDB (Seção 9)

```
$ aws dynamodb scan --table-name execucoes
```

```json
{
  "execution_id":    { "S": "normaliza-pedidos-spark-application-1790467853839" },
  "data_hora":       { "S": "2026-09-27T00:11:43.014471" },
  "dataset":         { "S": "pedidos_desnormalizado" },
  "linhas_lidas":    { "N": "110" },
  "linhas_gravadas": { "N": "103" },
  "status":          { "S": "SUCESSO" }
}
```

✅ Os seis campos do Requirement 8, com `execution_id` como chave de partição,
contagens como número (`N`) e `data_hora` em ISO-8601.

---

## 8. `terraform destroy` e conta limpa

```
$ terraform destroy

terraform_data.bucket_gold (local-exec): delete: s3://prova-bigdata-gold-3225002/fato_pedidos/data_pedido=2026-01-28/part-00000-...parquet
terraform_data.bucket_gold (local-exec): delete: s3://prova-bigdata-gold-3225002/jobs/normaliza_pedidos.py
terraform_data.bucket_gold (local-exec): delete: s3://prova-bigdata-gold-3225002/athena-results/...csv
terraform_data.bucket_gold (local-exec): remove_bucket: prova-bigdata-gold-3225002
aws_dynamodb_table.execucoes: Destruction complete after 8s

Destroy complete! Resources: 8 destroyed.
```

Conferência da conta após o destroy:

```
buckets S3 da prova   : (nenhum)
Glue jobs             : (nenhum)
Glue databases        : (nenhum)
Glue crawlers         : (nenhum)
DynamoDB              : (nenhuma)
Athena workgroups     : (só o primary)
```

> O `local-exec` com `when = destroy` esvazia o bucket antes de removê-lo. Sem
> isso o destroy falharia: os arquivos Parquet gravados pelo Glue e os
> resultados do Athena **não** são gerenciados pelo Terraform, e a AWS recusa
> apagar bucket não-vazio.

---

## 9. Observação sobre o enunciado

A Seção 10 do README instrui a "Complete os **TODOs** em `infra/gold.tf`" e
menciona "o padrão `terraform_data` + `local-exec` já fornecido como TODO em
`gold.tf`".

O arquivo `infra/gold.tf` **não está presente no repositório** — verifiquei o
estado atual, todo o histórico do git e a pasta de entrega de exemplo
`entregas/252525/`. O `terraform.tfvars.example` também não trazia as variáveis
`bucket_gold_nome`, `labrole_arn` e `tags` citadas na mesma seção.

Construí o `gold.tf` do zero e declarei as três variáveis, usando o `raw.tf`
como referência de estilo e a Seção 10 como especificação.
