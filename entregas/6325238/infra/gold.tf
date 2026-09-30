# gold.tf — CAMADA GOLD
# =============================================================================
# Infraestrutura da camada Gold:
# - Bucket S3 Gold
# - Upload do script do Glue
# - Glue Job
# - DynamoDB para metadados de execução
# - Glue Data Catalog
# - Glue Crawler
# - Athena Workgroup
#
# Learner Lab:
# O bucket S3 é criado via AWS CLI usando terraform_data, pois
# aws_s3_bucket pode tentar acessar Object Lock e sofrer SCP Deny.
# =============================================================================


# -----------------------------------------------------------------------------
# BUCKET GOLD
# -----------------------------------------------------------------------------

resource "terraform_data" "bucket_gold" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }

  triggers_replace = [
    var.bucket_gold_nome,
    var.regiao
  ]

  provisioner "local-exec" {
    command = <<-CMD
      set -e

      aws s3api create-bucket \
        --bucket "${var.bucket_gold_nome}" \
        --region "${var.regiao}" 2>/dev/null || true

      aws s3api put-public-access-block \
        --bucket "${var.bucket_gold_nome}" \
        --public-access-block-configuration \
        BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

      aws s3api put-bucket-tagging \
        --bucket "${var.bucket_gold_nome}" \
        --tagging 'TagSet=[
          {Key=Projeto,Value=ProvaBigData},
          {Key=Aluno,Value=Yuri Batista Sanches},
          {Key=RA,Value=6325238},
          {Key=Disciplina,Value=Big Data}
        ]'
    CMD
  }

  provisioner "local-exec" {
    when = destroy

    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}


# -----------------------------------------------------------------------------
# UPLOAD DO SCRIPT DO GLUE
# -----------------------------------------------------------------------------

resource "aws_s3_object" "glue_script" {
  bucket = var.bucket_gold_nome

  key = "scripts/normaliza_pedidos.py"

  source = "${path.module}/../glue-job/normaliza_pedidos.py"

  content_type = "text/x-python"

  depends_on = [
    terraform_data.bucket_gold
  ]
}


# -----------------------------------------------------------------------------
# DYNAMODB — METADADOS DE EXECUÇÃO
# -----------------------------------------------------------------------------

resource "aws_dynamodb_table" "execucoes" {
  name = var.dynamodb_tabela_nome

  billing_mode = "PAY_PER_REQUEST"

  hash_key = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = var.tags
}


# -----------------------------------------------------------------------------
# GLUE DATA CATALOG DATABASE
# -----------------------------------------------------------------------------

resource "aws_glue_catalog_database" "pedidos" {
  name = var.athena_database_nome

  description = "Data Catalog das tabelas Gold de pedidos."

  tags = var.tags
}


# -----------------------------------------------------------------------------
# GLUE JOB
# -----------------------------------------------------------------------------

resource "aws_glue_job" "normaliza_pedidos" {
  name = var.glue_job_nome

  role_arn = var.lab_role_arn

  glue_version = "4.0"

  worker_type = "G.1X"

  number_of_workers = 2

  timeout = 30

  max_retries = 0

  execution_class = "STANDARD"

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "false"

    "--RAW_PATH" = "s3://${var.bucket_raw_nome}/pedidos/"

    "--GOLD_PATH" = "s3://${var.bucket_gold_nome}/"

    "--DDB_TABLE" = var.dynamodb_tabela_nome

    "--DATASET_NAME" = "pedidos_desnormalizado"
  }

  tags = var.tags

  depends_on = [
    aws_s3_object.glue_script,
    aws_dynamodb_table.execucoes
  ]
}


# -----------------------------------------------------------------------------
# GLUE CRAWLER
# -----------------------------------------------------------------------------
# O crawler será executado depois que o Glue Job gerar os Parquets.
#
# Ele encontra:
#
#   gold/fato_pedidos/
#   gold/dim_cliente/
#   gold/dim_produto/
#
# e cria as tabelas no Glue Data Catalog para utilização pelo Athena.
# -----------------------------------------------------------------------------

resource "aws_glue_crawler" "pedidos_gold" {
  name = "crawler-pedidos-gold"

  role = var.lab_role_arn

  database_name = aws_glue_catalog_database.pedidos.name

  description = "Crawler das tabelas Gold de pedidos."

  table_prefix = ""

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
    delete_behavior = "LOG"

    update_behavior = "UPDATE_IN_DATABASE"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }

  tags = var.tags

  depends_on = [
    aws_glue_catalog_database.pedidos,
    aws_glue_job.normaliza_pedidos
  ]
}


# -----------------------------------------------------------------------------
# ATHENA WORKGROUP
# -----------------------------------------------------------------------------

resource "aws_athena_workgroup" "pedidos" {
  name = "wg-pedidos"

  description = "Workgroup Athena para consultas da camada Gold."

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  tags = var.tags

  depends_on = [
    terraform_data.bucket_gold
  ]
}

