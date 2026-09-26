output "glue_job_nome" {
  description = "Nome do Glue Job (aws glue start-job-run --job-name ...)."
  value       = aws_glue_job.normaliza.name
}

output "athena_workgroup" {
  description = "Workgroup do Athena onde rodar as consultas."
  value       = aws_athena_workgroup.prova.name
}

output "glue_database" {
  description = "Database do Glue Data Catalog."
  value       = aws_glue_catalog_database.prova.name
}

output "dynamodb_tabela" {
  description = "Tabela DynamoDB de metadados das execuções."
  value       = aws_dynamodb_table.execucoes.name
}

output "bucket_gold" {
  description = "Bucket gold."
  value       = var.bucket_gold_nome
}

output "bucket_raw" {
  description = "Bucket raw."
  value       = var.bucket_raw_nome
}

output "labrole_arn" {
  description = "ARN da LabRole usada pelo Glue."
  value       = var.labrole_arn
}
