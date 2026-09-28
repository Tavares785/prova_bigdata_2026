# Entrega — RA 6325149

Pipeline raw → Glue (PySpark) → gold (Parquet particionado) → Athena, com metadados no DynamoDB.
Código em `infra/` e `glue-job/`, testes em `local-test/`, evidências reais em `evidencias/`.

## Nota: tags de custo

As tags `Projeto`, `Disciplina` e `Ambiente` estão em todos os recursos provisionados por este
código: buckets gold e de resultados do Athena, Glue Job, database do Glue, Athena Workgroup e
DynamoDB. O bucket **raw** fica fora deste escopo — é criado por `infra/raw.tf`, template do
professor entregue "PRONTO (não altere)" (spec §3), que não chama `put-bucket-tagging`.
