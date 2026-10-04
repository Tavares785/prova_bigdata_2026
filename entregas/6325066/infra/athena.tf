# Database no AWS Glue Data Catalog para mapear as tabelas no Athena
resource "aws_glue_catalog_database" "bigdata_db" {
  name        = "db_pedidos_gold"
  description = "Database para consultar as tabelas normalizadas do bucket gold"
  tags        = var.tags
}

# Athena Workgroup para isolar a execução e direcionar os resultados
resource "aws_athena_workgroup" "analytics_wg" {
  name          = "wg_analytics_bigdata"
  description   = "Workgroup para as consultas da prova"
  state         = "ENABLED"
  force_destroy = true

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_resultados_nome}/athena-query-results/"
    }
  }

  tags       = var.tags
  depends_on = [terraform_data.bucket_gold]
}
