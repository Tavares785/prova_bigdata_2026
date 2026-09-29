# Entrega — Prova Prática de Big Data na AWS

**Aluno:** José Henrique Teixeira Luiz
**RA:** 3225002
**Curso:** Análise e Desenvolvimento de Sistemas — UniFAAT — 2026.2
**Executado em:** 26–27/09/2026 · AWS Academy Learner Lab, conta `447916381827`, `us-east-1`

Pipeline completo **executado na AWS e destruído ao final**:

```
S3 raw (CSV)  →  Glue Job PySpark  →  S3 gold (Parquet particionado)  →  Athena (SQL)
                        │
                        └──→  DynamoDB (metadados da execução)
```

## Resultado em uma tabela

| Etapa | Resultado |
|---|---|
| `terraform apply` | `8 added, 0 changed, 0 destroyed` |
| Glue Job | `SUCCEEDED` em 87 s |
| Modelo dimensional | **103** fatos · **21** clientes · **8** produtos |
| Particionamento | **24** partições `data_pedido=YYYY-MM-DD` |
| Consultas Athena | 4/4, com as validações cruzadas fechando |
| Integridade referencial | `orfaos = 0` |
| DynamoDB | os 6 campos do Requirement 8 |
| `terraform destroy` | `8 destroyed`, conta conferida limpa |

## O que está em cada arquivo

| Caminho | Conteúdo |
|---|---|
| [`EVIDENCIAS.md`](EVIDENCIAS.md) | **evidências de execução** — outputs reais do apply, do Job, das consultas, do DynamoDB e do destroy |
| [`evidencias/`](evidencias/) | **10 capturas de tela** do console AWS e do terminal ([índice](evidencias/README.md)) |
| [`RESULTADOS_ESPERADOS.md`](RESULTADOS_ESPERADOS.md) | o que o enunciado esperava × o que foi obtido, item por item |
| [`glue-job/normaliza_pedidos.py`](glue-job/normaliza_pedidos.py) | o Job de normalização, com as 5 funções implementadas |
| [`infra/gold.tf`](infra/gold.tf) | a camada gold — 7 recursos (construída do zero, ver nota abaixo) |
| [`infra/raw.tf`](infra/raw.tf) | a camada raw, fornecida pelo professor (não alterada) |
| [`infra/variables.tf`](infra/variables.tf) | variáveis, incluindo as 3 novas do gold |
| [`infra/terraform.tfvars.example`](infra/terraform.tfvars.example) | exemplo de valores (sem segredo) |
| [`sql/`](sql/) | as 4 consultas de referência da Seção 8 + validações adicionais, cada uma com o resultado obtido em comentário |
| [`tests/`](tests/) | a **bancada de testes local** — como validei a normalização antes de subir para a AWS ([como funciona](tests/README.md)) |
| [`dataset/`](dataset/) | o dataset de exemplo, para referência |

> **Não versionados, de propósito:** `terraform.tfvars` (carrega o ARN da conta),
> `terraform.tfstate`, `.terraform/` e qualquer credencial. As credenciais
> temporárias do Learner Lab vivem apenas em `~/.aws/credentials`.

## Como reproduzir

```bash
# 1. Learner Lab ligado, credenciais temporárias carregadas
aws sts get-caller-identity

# 2. ARN da LabRole
aws iam get-role --role-name LabRole --query 'Role.Arn' --output text

# 3. Preencher o tfvars
cp infra/terraform.tfvars.example infra/terraform.tfvars
#    ajustar bucket_raw_nome, bucket_gold_nome e labrole_arn

# 4. Um único apply cria raw + gold
cd infra
terraform init && terraform validate && terraform plan && terraform apply

# 5. Disparar o Job
aws glue start-job-run --job-name normaliza-pedidos

# 6. Catalogar e consultar
aws glue start-crawler --name cataloga-gold-pedidos
#    depois rodar os arquivos de sql/ no Athena, workgroup prova-bigdata-gold

# 7. OBRIGATÓRIO ao final
terraform destroy
```

## Duas decisões que valem explicação

**Os buckets não usam `aws_s3_bucket`.** A SCP da organização do Learner Lab nega
`s3:GetBucketObjectLockConfiguration`, que o recurso `aws_s3_bucket` sempre lê,
resultando em `AccessDenied`. Segui o padrão do `raw.tf` fornecido pelo
professor: criação via AWS CLI dentro do próprio `apply`, com
`terraform_data` + `local-exec`.

**O `destroy` esvazia o bucket antes de removê-lo.** Os arquivos Parquet que o
Glue grava e os resultados do Athena **não** são gerenciados pelo Terraform, e a
AWS recusa apagar bucket não-vazio. Sem o `local-exec` com `when = destroy`, o
`terraform destroy` falharia.

## Nota sobre o `infra/gold.tf`

A Seção 10 do README instrui a "completar os **TODOs** em `infra/gold.tf`" e
menciona o padrão `terraform_data` + `local-exec` "já fornecido como TODO em
`gold.tf`". O arquivo **não está presente no repositório** — verifiquei o estado
atual, todo o histórico do git e a pasta de exemplo `entregas/252525/`. O
`terraform.tfvars.example` também não trazia `bucket_gold_nome`, `labrole_arn`
nem `tags`.

Construí o `gold.tf` do zero e declarei as três variáveis, usando o `raw.tf`
como referência de estilo e a Seção 10 como especificação.
