# SPEC: Pipeline de Big Data de pedidos (raw → gold → Athena) na AWS  (v2 — pós-revisão red team)

## 1. Objetivo
Entregar a prova prática de Big Data (RA 6325149): provisionar via Terraform a camada gold, implementar o
Glue Job PySpark que normaliza `pedidos_desnormalizado.csv` em esquema estrela (1 fato + 2 dimensões),
gravar Parquet particionado, consultar via Athena e registrar metadados de execução no DynamoDB.

## 2. Contexto / Motivação (Why)
- Avaliação das Aulas 01–06 (100 pts, `RUBRICA.md`) no AWS Academy Learner Lab.
- Regra da prova: IA guia, não entrega a solução (`README.md`, `.kiro/steering`). O código de `.tf` e do Job
  é **autoria do aluno**; a IA faz spec/plano/tasks/revisão. Esta spec define só o quê/porquê.

## 3. Escopo
### Dentro
- Novos `.tf` em `infra/` + declarar variáveis novas em `variables.tf` e `terraform.tfvars.example`
  (o próprio `variables.tf` manda o aluno fazer isso). `raw.tf`, `versions.tf`, `dataset/`, `local-test/` intocados.
- `glue-job/normaliza_pedidos.py` (preencher `TODO(aluno)` e revisar `main()`).
- Evidências, entrega em `entregas/6325149/` (branch `prova-6325149`, fork, PR para `master`).
### Fora (NÃO fazer)
- Criar roles/policies IAM; usar `aws_s3_bucket`; extras além do enunciado.

## 4. Requisitos funcionais
- RF1: `fato_pedidos` (1 linha/`pedido_id`) com FKs e medidas, particionado por `data_pedido`.
- RF2: `dim_cliente` (chave única) e `dim_produto` (chave única), sem partição.
- RF3: descartar do fato linhas sem `pedido_id`/`cliente_id`/`produto_id` ou com `quantidade` ausente/`<= 0`.
- RF4: textos ausentes nas dimensões → `"DESCONHECIDO"`; linha da dimensão mantida.
- RF5: saída Parquet; layout `fato_pedidos/data_pedido=YYYY-MM-DD/`.
- RF6: 1 item/execução no DynamoDB: `execution_id` (S, único: UUID ou timestamp+random), `data_hora` (S, ISO-8601),
  `dataset` (S), `linhas_lidas` (N), `linhas_gravadas` (N), `status` (S: SUCESSO|FALHA). Falha grava FALHA
  sem mascarar a exceção original.
- RF7: tabelas no Glue Catalog **com as partições do fato visíveis ao Athena** (senão consultas 1, 3, 4 voltam vazias);
  workgroup com output location.
- RF8: as **4** consultas de referência retornam o esperado (README:320-369). O CHECKLIST diz "≥ 3"; README/rubrica
  pedem todas → cumprir as 4.
- RF9: re-execução do Job idempotente (não duplicar linhas no gold).

## 5. Restrições (vindas do enunciado — não são o "como" escolhido)
- `us-east-1`; provider aws 5.31.0; AWS CLI v2; buckets via `terraform_data`+CLI; LabRole por ARN.
- Glue `glueetl`, versão ≥ 4.0 (o esqueleto usa `dict[str, ...]`, exige Python ≥ 3.9); DynamoDB PAY_PER_REQUEST, PK `execution_id`.
- **Custo (regra absoluta):** Glue não tem free tier (cobra DPU-hora) → teto explícito de workers e timeout;
  Athena com limite de bytes escaneados no workgroup; Crawler (se usado) também custa DPU.
  Destroy imediato após o uso; nunca root.
- Segurança: buckets (gold **e** resultados Athena) privados com 4 bloqueios; nada de credenciais/`tfvars`/`tfstate` no git.
- Tags `Projeto`, `Disciplina`, `Ambiente` em todos os recursos que aceitam tag; para buckets criados via CLI,
  definir como tagear. Valores das tags: a decidir (ver §7).
- Lógica pura (`normalizar`, `montar_metadados`) separada do I/O.

## 6. Critérios de aceitação (verificáveis; números conferidos no CSV)
Dataset atual: 110 linhas; 7 inválidas (PED0096, PED0097, uma sem `pedido_id`, PED0099, PED0100, PED0101, PED0106).
- [ ] CA1: `terraform init/validate/plan/apply` sem erro; recursos esperados criados; `terraform fmt -check` limpo (CI).
- [ ] CA2: Job `SUCCEEDED`.
- [ ] CA3: `fato_pedidos` = **103** linhas; **24** partições `data_pedido`; chaves de dim_cliente e dim_produto sem duplicatas;
      produtos = 8; clientes = 16 (só válidas) ou 21 (todas) conforme decisão D2.
- [ ] CA4: consulta 1 sem `categoria` nula e soma = `SUM(valor_total)`; consulta 2 ≤ 5 linhas em ordem decrescente;
      consulta 3 com 24 linhas e `SUM(qtd_pedidos)` = 103; consulta 4 = 0.
- [ ] CA5: item DynamoDB com `linhas_lidas`=110, `linhas_gravadas`=103 (o exemplo 120/114 do README é ilustrativo).
- [ ] CA6: **Validação do Job do aluno:** os testes de `local-test` importam `normalizacao_referencia`
      (gabarito), **não** o seu script — verde ali não prova nada sobre o seu código. Definir como validar o seu
      `normalizar()` contra as mesmas propriedades (ver D5).
- [ ] CA7: bucket gold com 4 bloqueios confirmados (`aws s3api get-public-access-block`).
- [ ] CA8: destroy concluído **e verificado**: `raw.tf:43` usa `|| true`, então destroy "passa" mesmo com credencial
      expirada; conferir por `aws s3 ls`, `aws glue get-*`, `aws athena list-work-groups`, `aws dynamodb list-tables`.
- [ ] CA9: CHECKLIST 100%; `entregas/6325149/` montada; PR aberto sem `tfvars`/`tfstate`/credenciais.

## 7. Decisões pendentes (do aluno — a spec só pergunta, não responde)
- D1: PRD072/PRD084 têm atributo vazio numa linha e preenchido em outra. Qual vence: valor real ou "DESCONHECIDO"?
- D2: dimensões saem de todas as linhas ou só das válidas do fato? (afeta CA3 e integridade fato→dim)
- D3: vazio e só-espaços contam como ausente? (o gabarito trata como ausente)
- D4: modo de escrita/idempotência (append duplica; overwrite de partição tem semântica própria).
- D5: como validar o `normalizar()` do aluno localmente (ex.: rodar os mesmos casos contra o seu script).
- D6: onde subir o `.py` para `script_location`, e nome real da tabela DynamoDB vs `--DDB_TABLE` do exemplo.
- D7: valores das tags; estrutura de `entregas/6325149/` (o modelo `entregas/252525` copia dataset/glue-job/infra).
- D8: como registrar partições (Crawler vs alternativa) considerando custo.

## 8. Riscos / divergências conhecidas do repo
- README cita `infra/gold.tf`, `prova/infra` e `design.md` (inexistentes); `infra/README.md` manda criar do zero.
- README:414 lista `labrole_arn`/`bucket_gold_nome`/`tags`, mas `variables.tf` e `tfvars.example` só têm `regiao` e
  `bucket_raw_nome`; e diz "bucket_raw_nome do professor", mas a raw nasce na sua conta.
- `main()` do esqueleto: `execution_id` = JOB_NAME+applicationId (não é UUID/timestamp); `except` sem proteção
  pode mascarar o erro original; `job.commit()` fora do `try`. Investigar antes de rodar.
- `.gitignore` ignora `.terraform.lock.hcl`; CI usa `terraform_version: "1.16.1"` (conferir que existe).
- Steering ("manter NotImplementedError intactos") vs README ("preencha"): resolver com o professor.
- Rubrica critério 5 não cita `data_hora`; README/checklist sim (cumprir os três).
- Consulta 2 agrupa por `cliente_nome`: nomes iguais em IDs diferentes se fundem (checar no CSV).
- Glue local usa Spark 3.5.1; Glue 4.0 roda Spark 3.3 — cuidado com APIs.
