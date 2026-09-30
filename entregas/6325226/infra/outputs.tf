# outputs.tf — valores usados para disparar o job, rodar o crawler e consultar.

output "bucket_gold_nome" {
  description = "Bucket S3 gold (Parquet normalizado)."
  value       = var.bucket_gold_nome
}

output "glue_job_nome" {
  description = "Nome do Glue Job (aws glue start-job-run --job-name ...)."
  value       = aws_glue_job.normaliza_pedidos.name
}

output "glue_crawler_nome" {
  description = "Nome do Crawler (aws glue start-crawler --name ...), rodar após o job."
  value       = aws_glue_crawler.gold.name
}

output "glue_database" {
  description = "Database do Glue Data Catalog para selecionar no Athena."
  value       = aws_glue_catalog_database.gold.name
}

output "athena_workgroup" {
  description = "Workgroup do Athena com o local de resultados configurado."
  value       = aws_athena_workgroup.prova.name
}

output "dynamodb_tabela" {
  description = "Tabela DynamoDB com os metadados das execuções."
  value       = aws_dynamodb_table.execucoes.name
}

output "labrole_arn" {
  description = "ARN da LabRole usada pelo Glue Job e pelo Crawler."
  value       = data.aws_iam_role.labrole.arn
}
