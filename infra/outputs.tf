# outputs.tf — valores exibidos no final do terraform apply
# =============================================================================
# Outputs são o "retorno" do Terraform: aparecem no fim do apply e podem ser
# consultados a qualquer momento com `terraform output` (ou
# `terraform output -raw <nome>` para pegar só o valor, útil em scripts).
# =============================================================================

output "glue_job_nome" {
  description = "Nome do Glue Job de normalização."
  value       = aws_glue_job.normaliza_pedidos.name
}

output "comando_start_job_run" {
  description = "Comando pronto para disparar o Glue Job (usa os default_arguments do job)."
  value       = "aws glue start-job-run --job-name ${aws_glue_job.normaliza_pedidos.name} --region ${var.regiao}"
}

output "athena_workgroup" {
  description = "Workgroup a selecionar no console do Athena."
  value       = aws_athena_workgroup.prova.name
}

output "glue_database" {
  description = "Database do Glue Catalog com as tabelas do gold."
  value       = aws_glue_catalog_database.gold.name
}

output "dynamodb_tabela" {
  description = "Tabela DynamoDB com os metadados das execuções."
  value       = aws_dynamodb_table.execucoes.name
}
