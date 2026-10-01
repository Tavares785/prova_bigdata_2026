# PLAN v2: Pipeline de pedidos (spec.md v2 aprovada; revisado por agente red team)

Papéis: **IA** = spec/plan/tasks/guia/revisão. **Aluno** = escreve todo `.tf` e `.py`.

## Decisões adotadas
| # | Decisão |
|---|---|
| D1 | Valor real vence "DESCONHECIDO", escolha **determinística** (não depender de ordem). CLI018 (nome vazio, linha válida) → "DESCONHECIDO" |
| D2 | Dimensões só das linhas válidas (16 clientes, 8 produtos; CLI013–017 só existem em linhas inválidas) |
| D3 | Vazio e só-espaços = ausente |
| D4 | **Overwrite estático nas 3 tabelas** (gold inteiro reescrito). `partitionOverwriteMode=dynamic` deixa partições órfãs → não usar. Verificar com 2 runs (contagem + `s3 ls`) |
| D5 | Testes do aluno importam `glue-job/normaliza_pedidos.py`. Mecanismo: volume Docker (`-v`) ou `sys.path`/`importlib` por caminho; schema explícito igual ao de `ler_raw()`; `boto3` só dentro da função de I/O |
| D6 | `.py` sobe ao bucket gold (prefixo próprio) via `terraform_data`+CLI, com `triggers_replace = filemd5`; ordem: bucket → upload → Job |
| D7 | Tags `Projeto=prova-bigdata`, `Disciplina=big-data`, `Ambiente=lab`; buckets CLI via `put-bucket-tagging`; entrega `entregas/6325149/` |
| D8 | Registro de partições, em ordem de preferência: (c) partition projection na tabela do Catalog (custo 0; risco: formato/range de `date` errado → vazio silencioso); (b) `MSCK REPAIR` via Athena (custo ~0; precisa permissão); (a) Crawler só como último caso (DPU; rodar 1x, timeout curto, destruir) |
| D9 | Glue: `glue_version 4.0`, G.1X, 2 workers, timeout curto, `max_retries=0`. Athena: `bytes_scanned_cutoff_per_query`. `execution_id` = UUID ou timestamp+random |

## Fase A — Lógica local (custo zero, sem credenciais)
1. Testes D5 (contrato: 110→103; 24 partições; 8 produtos; 16 clientes; sem fato órfão; chave conflitante PRD072/084 → 1 linha; `valor_total` não nulo; CLI018 → DESCONHECIDO).
2. `normalizar()` (D1–D3) e `montar_metadados()` (campos, tipos N/S, ISO-8601).
3. Funções de I/O + `main()`: `execution_id` (D9); `try/except` interno ao gravar FALHA sem mascarar a exceção original; ordem de `job.commit()`; escrita D4; `boto3` tardio; sem APIs Spark ≥ 3.4 (Glue 4.0 = Spark 3.3, Python 3.10).
   Verificação: pytest verde no Docker (CA3, CA6).

## Fase B — Infra (sem apply)
4. Variáveis + `tfvars.example` (`labrole_arn`, `bucket_gold_nome`, `tags`, nome DynamoDB, nomes Glue/Athena).
5. Buckets gold e de resultados Athena (CLI): 4 bloqueios em **ambos**, `put-bucket-tagging`, destroy que esvazia e **não esconde falha**; upload do script (D6).
6. DynamoDB; Glue Job com LabRole, `default_arguments` (`--RAW_PATH/--GOLD_PATH/--DDB_TABLE/--DATASET_NAME`), D9; Catalog (D8); Athena Workgroup (`force_destroy`, limite de bytes).
7. Fecha com `terraform fmt -check`, **`init` real** (backend local + provider 5.31.0), `validate`, `plan` (CA1).

## Fase C — Learner Lab (custo; janela curta; `source ../aws-creds.sh`)
8. **Pré-voo** (repetir antes de apply, start-job-run e destroy): `aws sts get-caller-identity` — abortar se root/conta inesperada.
9. `apply`; conferir 4 bloqueios, tags (`list-tags`) — CA1/CA7. Teste manual de `dynamodb put-item` antes do Job (evita gastar DPU à toa).
10. `start-job-run` até `SUCCEEDED` (CA2). Segundo run para provar idempotência (D4). 
11. Se D8 = (b)/(a): registrar partições **agora** (dados só existem após o run). Conferir 24 partições e dims únicas (`s3 ls`, `COUNT(DISTINCT)`) — CA3.
12. 4 consultas Athena + `dynamodb scan` (`linhas_lidas=110`, `linhas_gravadas=103`) — CA4/CA5. Salvar saídas como evidência.
13. **Destroy imediato** + verificação de resíduos: `s3 ls`, `glue get-job/get-database/get-crawlers`, `athena list-work-groups`, `dynamodb list-tables`, log groups `/aws-glue/*` (CloudWatch) — CA8. Se sessão cair no meio: `terraform plan` antes do destroy; state parcial é esperado.

## Fase D — Entrega (confirmar com o Gabriel antes de push/fork/PR)
14. `entregas/6325149/` (decidir se `raw.tf` entra; modelo 252525 = dataset/glue-job/infra), CHECKLIST.
15. Varredura de segredos (`git diff --cached`, grep `AKIA|ASIA|aws_session_token`, sem `tfvars`/`tfstate`); decidir sobre `.terraform.lock.hcl` (está no .gitignore). Branch `prova-6325149`, commits convencionais.

## Mapa CA → verificação → evidência
CA1 fmt/init/validate/plan/apply → saída do terminal · CA2 `get-job-run` → estado · CA3 `s3 ls`+Athena distinct + pytest · CA4 4 consultas → prints · CA5 `dynamodb scan` → JSON · CA6 pytest do aluno · CA7 `get-public-access-block` (gold+resultados) · CA8 comandos da task 13 → saídas vazias · CA9 varredura da task 15 + CHECKLIST.

## Riscos residuais
- CI (`ci.yml`) só roda `local-test/` (gabarito) e usa `terraform 1.16.1` (conferir); testes do aluno não rodam no CI e não alteramos o workflow.
- `raw.tf:43` esconde falha com `|| true`: arquivo protegido → verificação por CLI é obrigatória.
- Consulta 2 agrupa por nome: `DESCONHECIDO` (CLI018) pode fundir clientes — documentar.
- Nenhum recurso fica de pé entre sessões.
