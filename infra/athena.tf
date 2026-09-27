resource "aws_glue_catalog_database" "analytics" {
  name = "prova_bigdata"
}

resource "aws_athena_workgroup" "analytics" {
  name = "prova-bigdata"

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  tags = var.tags
}