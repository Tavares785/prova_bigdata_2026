# Banco de Dados no Glue Data Catalog
resource "aws_glue_catalog_database" "banco_ecommerce" {
  name = "ecommerce_db" # Nome simples, sem hífens
}

# Glue Crawler
resource "aws_glue_crawler" "crawler_pedidos" {
  name = "crawler-pedidos-gold"
  role = var.labrole_arn

  database_name = aws_glue_catalog_database.banco_ecommerce.name

  s3_target {
    path = "s3://${var.bucket_gold_nome}/pedidos_normalizados/fato_pedidos/"
  }

  s3_target {
    path = "s3://${var.bucket_gold_nome}/pedidos_normalizados/dim_cliente/"
  }

  s3_target {
    path = "s3://${var.bucket_gold_nome}/pedidos_normalizados/dim_produto/"
  }

  tags = var.tags
}

# Athena Workgroup
resource "aws_athena_workgroup" "workgroup_lab" {
  name = "workgroup_lab_ecommerce"

  force_destroy = true

  configuration {
    result_configuration {
      # Pasta separada no bucket gold apenas para os logs das consultas
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  tags = var.tags
}