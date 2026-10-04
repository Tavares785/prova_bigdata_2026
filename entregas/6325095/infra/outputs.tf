# outputs.tf — infra (prova)
# Saídas úteis para disparar o Glue Job/Crawler e rodar as consultas Athena
# sem precisar ir ao console procurar os nomes dos recursos.

output "bucket_raw_nome" {
  description = "Bucket S3 raw (camada de origem)."
  value       = var.bucket_raw_nome
}

output "bucket_gold_nome" {
  description = "Bucket S3 gold (camada normalizada)."
  value       = var.bucket_gold_nome
}

output "glue_job_name" {
  description = "Nome do Glue Job de normalização (aws glue start-job-run --job-name <valor>)."
  value       = aws_glue_job.normaliza_pedidos.name
}

output "glue_database_name" {
  description = "Database do Glue Data Catalog com as tabelas do gold."
  value       = aws_glue_catalog_database.gold.name
}

output "glue_crawler_name" {
  description = "Nome do Crawler (aws glue start-crawler --name <valor>) — rodar após o Glue Job."
  value       = aws_glue_crawler.gold.name
}

output "athena_workgroup_name" {
  description = "Athena Workgroup a usar nas consultas de referência."
  value       = aws_athena_workgroup.prova.name
}

output "dynamodb_table_name" {
  description = "Tabela DynamoDB de metadados das execuções."
  value       = aws_dynamodb_table.execucoes.name
}
