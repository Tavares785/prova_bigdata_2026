# Entrega — Prova Big Data AWS — RA 6322006

## Aluno
- **Nome:** Rafael Nogueira Maruca
- **RA:** 6322006
- **Branch:** `prova-6322006`

## Entregáveis (todos em `entregas/6322006/`)
| Item | Caminho |
|---|---|
| Terraform (raw pronto + gold construído) | `infra/gold.tf`, `infra/variables.tf`, `infra/terraform.tfvars.example` (+ `raw.tf` e `versions.tf` sem alteração de conteúdo) |
| Script PySpark do Glue Job | `glue-job/normaliza_pedidos.py` |
| Dataset (cópia do original, necessário para o `../dataset` do `raw.tf`) | `dataset/` |
| Evidências (prints + log) | `evidencias/` |

## Evidências × critérios da rubrica
| Critério | Evidências |
|---|---|
| 1. Terraform aplica sem erro (20) | `01d` init + validate · `01a` apply (conclusão após correção de fim de linha — ver Observações) · `01b` state list (8 recursos) · `01c` plan "No changes" |
| 2. Normalização correta (25) | `00` **pytest com o MEU script montado no lugar do gabarito (`-v`) → 11 passed** (+ `.txt` com a saída completa) · `08c` 103 linhas no fato · `09` 0 órfãos · `06` nenhuma categoria nula nem `DESCONHECIDO` indevido |
| 3. Parquet particionado por `data_pedido` (15) | `04` 24 pastas `data_pedido=YYYY-MM-DD` · `05` PartitionKeys no catálogo |
| 4. Consultas Athena (15) | `06` consulta 1 · `07` consulta 2 · `08a`/`08b` consulta 3 (24 linhas) · `08c` total do fato (103 / 61798.9 = soma das categorias) · `09` consulta 4 |
| 5. Metadados no DynamoDB (10) | `10` item com os 6 campos (linhas_lidas 110 N, linhas_gravadas 103 N, status SUCESSO) · `03` Glue Job SUCCEEDED |
| 6. Custo/limpeza e segurança (15) | `02a`/`02b` buckets com os 4 bloqueios de acesso público · `11` recursos com a tag `Projeto` (Glue Job, Crawler, Athena, DynamoDB) · LabRole por ARN (`var.labrole_arn`, sem IAM próprio) · `12` destroy (8 destroyed) + verificação vazia |

## Números esperados × obtidos (calculados antes, a partir do CSV)
| Métrica | Previsto | Obtido |
|---|---|---|
| Linhas lidas do raw | 110 | 110 |
| Linhas no fato (7 inválidas descartadas) | 103 | 103 |
| Partições `data_pedido` | 24 | 24 |
| Órfãos fato → dim_produto | 0 | 0 |

## Decisões técnicas
- **Limpeza antes da normalização:** `trim` e texto vazio/espaços → nulo em todas as colunas de texto (os testes geram `None`, `""` e `"   "`).
- **Dimensões com `max()` (e não `first()`):** determinístico e ignora nulos; ex.: PRD084 fica "Mochila Escolar" mesmo com uma linha de nome vazio. `DESCONHECIDO` só quando não existe nenhum valor preenchido.
- **Dimensões a partir de todas as linhas com chave da própria dimensão:** um cliente com pedido inválido continua no cadastro (dimensão = cadastro; fato = eventos). Integridade fato → dimensão garantida (consulta 4 = 0).
- **Esquema explícito na leitura** (em vez de `inferSchema`): tipos garantidos (`data_pedido` date, medidas numéricas) e uma leitura a menos.
- **`mode("overwrite")`** na gravação: reexecutar o Job não duplica nem falha.
- **Crawler com a LabRole** (um `s3_target` por tabela, não varre `scripts/` nem `athena-results/`).
- **Script no S3 via `terraform_data` separado** com `filemd5()` no trigger (corrigir o script não recria o bucket).
- **`--DDB_TABLE` referenciando `aws_dynamodb_table.execucoes.name`** (nome nunca diverge).
- **Glue 5.0 (Spark 3.5)**, mesmo Spark dos testes locais; G.1X, 2 workers, timeout 10 min.

## Observações de execução
- O `raw.tf` usa comandos bash no `local-exec`; no Windows o Terraform usaria `cmd`. Por isso executei o Terraform em um **container Linux** (imagem `hashicorp/terraform` + `aws-cli`).
- No 1º `apply`, 7 de 8 recursos foram criados e o `bucket_raw` falhou com `set: illegal option -`: o Git do Windows (`core.autocrlf=true`) converteu os `.tf` para **CRLF** e o `sh` leu o `\r`. Não houve alteração de conteúdo do `raw.tf` (`git ls-files --eol`: `i/lf w/crlf`). Corrigi com `core.autocrlf input` + `dos2unix`; o novo `apply` recriou só o recurso que faltava (print `01a`), e o estado final foi confirmado pelo `state list` e pelo `plan` sem mudanças (`01b`, `01c`).
- Após a execução, alterei apenas **comentários** do script (cabeçalho e o comentário do passo 3); a lógica é a mesma que rodou no Glue e passou nos testes (`00`).
- **Tags no database e no bucket gold — adicionadas depois da execução, NÃO executadas:** após a execução completa,
  acrescentei `tags = var.tags` no `aws_glue_catalog_database` e o recurso `terraform_data.bucket_gold_tags`
  (`aws s3api put-bucket-tagging`, com o TagSet gerado a partir de `var.tags`). O código passa no
  `terraform validate`, mas **não pôde ser aplicado nem evidenciado**, porque o acesso ao AWS Academy Learner Lab
  foi encerrado ("This course has ended"). Por isso as evidências mostram 8 recursos (versão executada) e o código
  final declara 9.

## Uso de IA
Declaro o uso de IA nesta prova:
- **Claude (assistente de IA):** parte do código (funções do `normaliza_pedidos.py` e recursos do `gold.tf`) foi feita com o Claude. Ele também explicou os conceitos, ajudou a interpretar os erros, conferiu os resultados e redigiu o texto deste README / descrição do PR.
- **Outra IA:** usada para ajudar com os comandos de PowerShell.
- **Minha parte:** revisei o código, executei os testes locais, rodei o Terraform, o Glue Job, o Crawler e as consultas na AWS, conferi os resultados e fiz as evidências.

## Checklist (CHECKLIST.md)
- [x] Credenciais nunca versionadas; só `*.tfvars.example`
- [x] Região us-east-1; AWS CLI v2; `init`/`validate`/`apply` concluídos
- [x] Bucket gold privado; LabRole por ARN
- [x] Glue Job SUCCEEDED; Parquet particionado por `data_pedido`
- [x] Tabelas no catálogo; 4 consultas Athena conferidas
- [x] Item de metadados no DynamoDB
- [ ] Tags em todos os recursos — **evidenciadas** em Glue Job, Crawler, Athena e DynamoDB; no database e no bucket gold estão **no código, mas não executadas** (lab encerrado — ver Observações). O bucket raw é da camada pronta do professor (`raw.tf`, não alterado).
- [x] `terraform destroy` executado (evidência `12`)
- [x] Fork, branch `prova-6322006`, pasta `entregas/6322006/`
