# TASKS v2 (revisado contra spec v2, plan v2, README, CHECKLIST, RUBRICA e CI). [x] só após verificação real.

## Fase 0 — Pré-requisitos (sem custo)
- [x] T00 Ferramentas: `aws --version` (v2), `terraform version` (>= 1.5), `docker --version`, `python3 --version`.
      Pronto: todas respondem. (CHECKLIST §2: AWS CLI v2 obrigatório.)

## Fase A — Lógica local (sem AWS)
- [x] T01 Testes do aluno (D5): importam `glue-job/normaliza_pedidos.py`, schema explícito, CSV real. Cobrem: contagens
      (110→103, 24 datas, 8 produtos, 16 clientes), sem órfãos, 1 linha por chave (PRD072/084), CLI018=DESCONHECIDO,
      `montar_metadados` (tipos/ISO-8601), e caminho de **FALHA** do `main()` com AWS simulado.
      Pronto: falham com `NotImplementedError` antes da T02.
- [x] T02 `normalizar()` (RF2–RF4, D1–D3). Pronto: testes de contagem/chave/órfãos verdes.
- [x] T03 `montar_metadados()` (RF6). Pronto: 6 campos, N vs S, `execution_id` único.
- [x] T04 `ler_raw()` e `escrever_gold()` (RF5, RF9, D4). **Sobrescrever apenas os caminhos de cada tabela
      (`fato_pedidos/`, `dim_*`), nunca a raiz do gold** (o script do Glue vive no mesmo bucket, D6).
      Pronto: 24 partições; 2ª execução local sem duplicar.
- [x] T05 `gravar_metadados_dynamo()` + `main()` (except interno, `commit()`, `execution_id`, `boto3` tardio, sem APIs ≥ 3.4).
      Pronto: pytest verde no Docker (CA6).

## Fase B — Infra (sem apply)
- [x] T06 Variáveis + `tfvars.example` (labrole_arn, bucket_gold_nome, tags, nomes). Pronto: `validate` ok.
- [x] T07 Buckets gold e resultados Athena via CLI: 4 bloqueios, `put-bucket-tagging`, destroy que esvazia e falha visível.
- [x] T08 Upload do script (`filemd5`) + DynamoDB (PK `execution_id` S, PAY_PER_REQUEST, tags).
- [x] T09 Glue Job (LabRole por ARN, `default_arguments` dos 4 params, D9, `depends_on` do upload, tags).
- [x] T10 Catalog: database + **3 tabelas** (fato particionada, dim_cliente, dim_produto) com o **schema real do
      Parquet da T04** + D8 (partition projection). Athena Workgroup (limite de bytes, `force_destroy`, tags).
- [x] T11 `terraform fmt -check -recursive`, `init` real, `validate`, `plan` (CA1). **O CI só valida `infra/` na raiz
      do repositório**: o código vive em `infra/` e é copiado para `entregas/6325149/` (T21).

## Fase C — Learner Lab (janela curta; sessão pode expirar: se cair, `terraform plan`/`destroy` antes de tudo)
- [x] T12 Pré-voo: `source ../aws-creds.sh` + `sts get-caller-identity` (não root). Criar `terraform.tfvars` real
      (LabRole ARN via `aws iam get-role --role-name LabRole`), nunca commitar. Repetir pré-voo antes de T13, T15, T20.
- [x] T13 `apply` (CA1); 4 bloqueios (gold + resultados) e tags conferidos (CA7).
- [x] T14 `put-item` manual de teste no DynamoDB e remoção do item.
- [x] T15 `start-job-run` até SUCCEEDED (CA2); 2º run prova idempotência (o DynamoDB terá 2 itens, ambos SUCESSO).
- [x] T16 Partições (24) visíveis e dims únicas (CA3).
- [x] T17 4 consultas Athena **no workgroup criado** (CA4); salvar saídas.
- [x] T18 `dynamodb scan`: `linhas_lidas=110`, `linhas_gravadas=103`, `status=SUCESSO`, `data_hora` presente (CA5).
- [x] T19 Evidências: prints/saídas de apply, consultas, DynamoDB **e** (na T20) do destroy.
- [x] T20 **`terraform destroy`** + verificação de resíduos incl. CloudWatch logs; salvar evidência do destroy (CA8, rubrica 6).

## Fase D — Entrega (confirmar com o Gabriel antes de push/fork/PR)
- [x] T21 Montar `entregas/6325149/` (dataset, glue-job, infra; decidir sobre `raw.tf`), evidências, CHECKLIST 100%.
- [x] T22 Varredura de segredos (`AKIA|ASIA|aws_session_token`, sem `tfvars`/`tfstate`); decisão sobre `.terraform.lock.hcl`;
      branch `prova-6325149`; commits convencionais.
- [ ] T23 Fork + push + PR para `master` (só com autorização explícita).

## Mapa de cobertura (nada sem task)
RF1–RF5 → T02, T04 · RF6 → T03, T05 · RF7 → T10, T16 · RF8 → T17 · RF9 → T04, T15 ·
CA1 → T11, T13 · CA2 → T15 · CA3 → T02, T16 · CA4 → T17 · CA5 → T18 · CA6 → T01–T05 · CA7 → T13 ·
CA8 → T20 · CA9 → T21–T23 · CHECKLIST §1 → T12, T22 · §2 → T00, T11 · §3 → T07, T09 · §4 → T15 ·
§5 → T10, T17 · §6 → T18 · §7 → T13, T20 · §8 → T21–T23.
