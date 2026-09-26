# Entrega — RA 6325149

Pipeline raw → Glue (PySpark) → gold (Parquet particionado) → Athena, com metadados no DynamoDB.
Código em `infra/` e `glue-job/`, testes em `local-test/`, evidências reais em `evidencias/`.

## Aviso: tags no bucket raw

O bucket raw **não recebeu as tags** de custo (`Projeto`, `Disciplina`, `Ambiente`). Ele é criado pelo
`infra/raw.tf`, que o enunciado marca como **"PRONTO (não altere)"**; esse arquivo não chama
`put-bucket-tagging`, e por isso o `raw.tf` foi mantido intacto. Todos os demais recursos criados
por este código (buckets gold e de resultados do Athena, Glue Job, database do Glue, Athena
Workgroup e DynamoDB) têm as três tags.
