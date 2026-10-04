# gold.tf — camada Gold e serviços analíticos
# =============================================================================
# Infraestrutura construída pelo aluno:
# - Bucket Gold
# - Upload do script PySpark
# - Glue Job
# - DynamoDB
# - Glue Data Catalog / Crawler
# - Athena Workgroup
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
    var.regiao,
  ]

  provisioner "local-exec" {
    command = <<-CMD
      set -e

      aws s3api create-bucket \
        --bucket "${self.input.bucket}" \
        --region "${self.input.regiao}" 2>/dev/null || true

      aws s3api put-public-access-block \
        --bucket "${self.input.bucket}" \
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
    CMD
  }

  # DESTROY: esvazia e remove o Bucket_Gold no terraform destroy.
  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}


# -----------------------------------------------------------------------------
# SCRIPT DO GLUE
# -----------------------------------------------------------------------------

resource "terraform_data" "script_glue" {
  input = {
    arquivo = "${path.module}/../glue-job/normaliza_pedidos.py"
    destino = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
  }

  triggers_replace = [
    filesha256("${path.module}/../glue-job/normaliza_pedidos.py"),
  ]

  provisioner "local-exec" {
    command = <<-CMD
      set -e

      aws s3 cp \
        "${self.input.arquivo}" \
        "${self.input.destino}"
    CMD
  }

  depends_on = [
    terraform_data.bucket_gold,
  ]
}


# -----------------------------------------------------------------------------
# DYNAMODB — METADADOS DAS EXECUÇÕES
# -----------------------------------------------------------------------------

resource "aws_dynamodb_table" "execucoes" {
  name         = "execucoes"
  billing_mode = "PAY_PER_REQUEST"

  hash_key = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = var.tags
}


# -----------------------------------------------------------------------------
# GLUE JOB
# -----------------------------------------------------------------------------

resource "aws_glue_job" "normaliza_pedidos" {
  name     = "normaliza-pedidos"
  role_arn = var.labrole_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }

  glue_version = "4.0"

  worker_type       = "G.1X"
  number_of_workers = 2

  default_arguments = {
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"    = aws_dynamodb_table.execucoes.name
    "--DATASET_NAME" = "pedidos_desnormalizado"
  }

  tags = var.tags

  depends_on = [
    terraform_data.script_glue,
  ]
}


# -----------------------------------------------------------------------------
# GLUE DATA CATALOG
# -----------------------------------------------------------------------------

resource "aws_glue_catalog_database" "gold" {
  name = "prova_bigdata_gold"
}


# -----------------------------------------------------------------------------
# GLUE CRAWLER
# -----------------------------------------------------------------------------

resource "aws_glue_crawler" "gold" {
  name          = "crawler-gold"
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

  tags = var.tags

  depends_on = [
    terraform_data.bucket_gold,
  ]
}


# -----------------------------------------------------------------------------
# ATHENA WORKGROUP
# -----------------------------------------------------------------------------

resource "aws_athena_workgroup" "gold" {
  name          = "prova-bigdata-gold"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  tags = var.tags

  depends_on = [
    terraform_data.bucket_gold,
  ]
}
