resource "aws_glue_catalog_database" "gold" {
  name = "prova_bigdata_6325197"
  tags = var.tags
}

resource "aws_glue_crawler" "gold" {
  name          = "crawler-gold-6325197"
  database_name = aws_glue_catalog_database.gold.name
  role          = var.labrole_arn

  s3_target {
    path = "s3://${var.bucket_gold_nome}/fato_pedidos/"
  }

  s3_target {
    path = "s3://${var.bucket_gold_nome}/dim_cliente/"
  }

  s3_target {
    path = "s3://${var.bucket_gold_nome}/dim_produto/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  tags = var.tags

  depends_on = [terraform_data.bucket_gold]
}

resource "aws_athena_workgroup" "prova" {
  name          = "prova-6325197"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = var.tags

  depends_on = [terraform_data.bucket_gold]
}

output "catalogo_database" {
  value = aws_glue_catalog_database.gold.name
}

output "crawler_nome" {
  value = aws_glue_crawler.gold.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.prova.name
}

output "bucket_raw_nome" {
  value = var.bucket_raw_nome
}

output "bucket_gold_nome" {
  value = var.bucket_gold_nome
}
