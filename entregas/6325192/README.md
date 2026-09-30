# Prova Prática de Big Data — RA 6325192

Aluno: Emar Cristian Silva Teruo Ito
RA: 6325192
Branch: prova-6325192

## Implementação

- S3 Raw
- S3 Gold privado
- AWS Glue Job PySpark
- Modelo dimensional:
  - fato_pedidos
  - dim_cliente
  - dim_produto
- Parquet particionado por data_pedido
- Glue Data Catalog / Crawler
- Athena Workgroup
- DynamoDB para metadados de execução

## Evidências

A pasta `evidencias/` contém os registros de:

- Terraform plan/apply
- execução do Glue Job
- conteúdo e partições do S3 Gold
- Glue Data Catalog
- consultas Athena
- item de metadados no DynamoDB
- Terraform destroy

Credenciais temporárias e `terraform.tfvars` não são versionados.
