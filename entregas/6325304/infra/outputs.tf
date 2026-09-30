output "bucket_raw" {
  value = var.bucket_raw_nome
}

output "bucket_gold" {
  value = var.bucket_gold_nome
}

output "glue_job" {
  value = aws_glue_job.normalizacao.name
}

output "glue_crawler" {
  value = aws_glue_crawler.gold.name
}

output "glue_database" {
  value = aws_glue_catalog_database.gold.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.wg.name
}

output "dynamodb_tabela" {
  value = aws_dynamodb_table.catalogo.name
}
