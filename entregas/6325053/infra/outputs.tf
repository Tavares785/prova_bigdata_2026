output "bucket_raw" { value = var.bucket_raw_nome }
output "bucket_gold" { value = var.bucket_gold_nome }
output "glue_job" { value = aws_glue_job.normaliza.name }
output "database" { value = aws_glue_catalog_database.gold.name }
output "athena_workgroup" { value = aws_athena_workgroup.gold.name }
output "dynamodb_table" { value = aws_dynamodb_table.execucoes.name }
