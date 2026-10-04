# outputs.tf — infra (prova)
# Saídas da camada gold (escrita pelo ALUNO, RA 6325269).
# Valores exibidos no fim do apply e lidos na Fase 4 com
# terraform output -raw <nome>.
# Nenhum recurso é criado aqui; o arquivo só expõe nomes e caminhos.

output "bucket_gold_nome" {
  description = "Nome global único do bucket S3 gold."
  value       = var.bucket_gold_nome
}

output "gold_path" {
  description = "Caminho S3 raiz da camada gold."
  value       = local.gold_path
}

output "glue_job_nome" {
  description = "Nome do job ETL do AWS Glue (raw -> gold)."
  value       = aws_glue_job.normaliza_pedidos.name
}

output "dynamodb_tabela_nome" {
  description = "Nome da tabela DynamoDB com os metadados de cada execução do Glue Job."
  value       = aws_dynamodb_table.execucoes.name
}

output "glue_database_nome" {
  description = "Nome do database no Glue Data Catalog."
  value       = aws_glue_catalog_database.gold.name
}

output "athena_workgroup_nome" {
  description = "Nome do workgroup do Amazon Athena."
  value       = aws_athena_workgroup.gold.name
}
