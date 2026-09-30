# gold.tf - camada gold, Glue, Catalog, Athena e DynamoDB

locals {
  glue_job_nome  = "job-normaliza-pedidos"
  glue_db_nome   = "prova_bigdata_gold"
  crawler_nome   = "crawler-gold-pedidos"
  workgroup_nome = "wg-prova-bigdata"
  ddb_nome       = "catalogo-execucoes"
  dataset_nome   = "pedidos_desnormalizado"
  script_key     = "scripts/normaliza_pedidos.py"
}

# Bucket GOLD (criado via AWS CLI, mesmo padrao do raw.tf)
resource "terraform_data" "bucket_gold" {
  input = {
    nome   = var.bucket_gold_nome
    regiao = var.regiao
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      set -e
      aws s3api create-bucket --bucket ${self.input.nome} --region ${self.input.regiao}
      aws s3api put-public-access-block --bucket ${self.input.nome} \
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
      aws s3api put-bucket-tagging --bucket ${self.input.nome} \
        --tagging '${jsonencode({ TagSet = [for k, v in var.tags : { Key = k, Value = v }] })}'
    EOT
  }

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["/bin/bash", "-c"]
    command     = "aws s3 rb s3://${self.input.nome} --force --region ${self.input.regiao} || true"
  }
}

# Script do Glue Job enviado para o bucket gold
resource "aws_s3_object" "script_glue" {
  bucket = var.bucket_gold_nome
  key    = local.script_key
  source = "${path.module}/../glue-job/normaliza_pedidos.py"
  etag   = filemd5("${path.module}/../glue-job/normaliza_pedidos.py")
  tags   = var.tags

  depends_on = [terraform_data.bucket_gold]
}

# DynamoDB: catalogo de execucoes
resource "aws_dynamodb_table" "catalogo" {
  name         = local.ddb_nome
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = var.tags
}

# Glue Job
resource "aws_glue_job" "normalizacao" {
  name              = local.glue_job_nome
  role_arn          = var.labrole_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 15

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.bucket_gold_nome}/${local.script_key}"
  }

  default_arguments = {
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/pedidos_desnormalizado.csv"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"    = local.ddb_nome
    "--DATASET_NAME" = local.dataset_nome
    "--job-language" = "python"
  }

  tags = var.tags

  depends_on = [aws_s3_object.script_glue, aws_dynamodb_table.catalogo]
}

# Glue Data Catalog: database + crawler
resource "aws_glue_catalog_database" "gold" {
  name = local.glue_db_nome
}

resource "aws_glue_crawler" "gold" {
  name          = local.crawler_nome
  role          = var.labrole_arn
  database_name = aws_glue_catalog_database.gold.name

  s3_target {
    path = "s3://${var.bucket_gold_nome}/fato_pedidos/"
  }

  s3_target {
    path = "s3://${var.bucket_gold_nome}/dim_cliente/"
  }

  s3_target {
    path = "s3://${var.bucket_gold_nome}/dim_produto/"
  }

  tags = var.tags

  depends_on = [terraform_data.bucket_gold]
}

# Athena Workgroup
resource "aws_athena_workgroup" "wg" {
  name          = local.workgroup_nome
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false

    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  tags = var.tags

  depends_on = [terraform_data.bucket_gold]
}
