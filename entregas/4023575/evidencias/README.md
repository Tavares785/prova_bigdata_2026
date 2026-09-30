# Evidências de Execução — Prova Big Data
  **Aluna:** Emilly Santos de Oliveira
  **RA:** 4023575
  **Data:** 30/09/2026

  ## Arquivos

  | Arquivo | Descrição |
  |---|---|
  | 01_terraform_apply_state.txt | Recursos criados pelo terraform apply |
  | 02_glue_job_succeeded.json | Glue Job com status SUCCEEDED |
  | 03_athena_q1.json | Faturamento por categoria |
  | 04_athena_q2.json | Top 5 clientes por gasto |
  | 05_athena_q3.json | Pedidos e faturamento por dia (24 partições) |
  | 06_athena_q4.json | Integridade referencial — orfaos = 0 |
  | 07_dynamodb.json | Item de metadados no DynamoDB |
  | 08_terraform_destroy.txt | Evidência do terraform destroy |

  ## Resultados

  - **Testes locais:** 11/11 passando
  - **Glue Job:** SUCCEEDED — 110 linhas lidas, 103 gravadas
  - **Athena Q1:** Eletronicos R$30.334,10 | Moveis R$13.485,00 | Calcados
  R$5.698,10
  - **Athena Q2:** Top cliente: Carla Nunes R$6.955,20
  - **Athena Q3:** 24 partições (2026-01-05 a 2026-01-28)
  - **Athena Q4:** orfaos = 0 ✅
  - **DynamoDB:** status SUCESSO ✅