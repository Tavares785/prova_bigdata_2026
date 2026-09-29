# outputs.tf — camada gold (prova-bigdata-aws)
# Outputs úteis pós-apply. Req 13.1.9.

output "glue_job_nome" {
  description = "Nome do Glue Job de normalização."
  value       = aws_glue_job.normaliza_pedidos.name
}

output "glue_database_nome" {
  description = "Nome do database no Glue Data Catalog."
  value       = aws_glue_catalog_database.prova.name
}

output "athena_workgroup_nome" {
  description = "Nome do Athena Workgroup."
  value       = aws_athena_workgroup.prova.name
}

output "dynamodb_table_nome" {
  description = "Nome da tabela DynamoDB de metadados de execução."
  value       = aws_dynamodb_table.execucoes.name
}

output "s3_raw_path" {
  description = "Caminho S3 do Bucket_Raw (prefixo do dataset de entrada)."
  value       = "s3://${var.bucket_raw_nome}/pedidos/"
}

output "s3_gold_path" {
  description = "Caminho S3 do Bucket_Gold (prefixo raiz da camada gold)."
  value       = "s3://${var.bucket_gold_nome}/"
}
