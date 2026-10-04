# outputs.tf — infra (prova): valores da sessão do Learner Lab (Glue Job, MSCK/Athena, DynamoDB).

output "bucket_raw" {
  description = "Nome do Bucket_Raw (origem do dataset)."
  value       = var.bucket_raw_nome
}

output "bucket_gold" {
  description = "Nome do Bucket_Gold (gold + script do Glue + resultados do Athena)."
  value       = var.bucket_gold_nome
}

output "glue_job_name" {
  description = "Nome do Glue Job a disparar com 'aws glue start-job-run'."
  value       = aws_glue_job.normaliza_pedidos.name
}

output "catalog_database" {
  description = "Database do Glue Data Catalog que contém as tabelas do gold."
  value       = aws_glue_catalog_database.gold.name
}

output "glue_script_location" {
  description = "Local no S3 do script PySpark usado pelo Glue Job."
  value       = "${local.gold_base}/scripts/normaliza_pedidos.py"
}

output "athena_workgroup" {
  description = "Workgroup do Athena (output_location já apontando para o gold)."
  value       = aws_athena_workgroup.prova.name
}

output "dynamodb_table" {
  description = "Tabela DynamoDB de metadados das execuções."
  value       = aws_dynamodb_table.metadados.name
}

output "msck_hint" {
  description = "Comando a rodar no Athena assim que o Glue Job concluir."
  value       = "MSCK REPAIR TABLE ${aws_glue_catalog_database.gold.name}.fato_pedidos"
}