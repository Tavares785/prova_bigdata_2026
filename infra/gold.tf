# gold.tf — CAMADA GOLD (construída pelo aluno)

# ---------- Bucket Gold (via CLI, mesmo padrão do raw.tf) ----------
resource "terraform_data" "bucket_gold" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }

  triggers_replace = [var.bucket_gold_nome, var.regiao]

  provisioner "local-exec" {
    command = <<-CMD
      set -e
      aws s3api create-bucket --bucket "${var.bucket_gold_nome}" --region "${var.regiao}" 2>/dev/null || true
      aws s3api put-public-access-block --bucket "${var.bucket_gold_nome}" \
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
      aws s3 cp "${path.module}/../glue-job/normaliza_pedidos.py" \
        "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    CMD
  }

  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}

# ---------- Glue Job ----------
resource "aws_glue_job" "normaliza_pedidos" {
  name     = "normaliza-pedidos"
  role_arn = var.labrole_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 10

  default_arguments = {
    "--job-language" = "python"
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"    = var.dynamodb_table_nome
    "--DATASET_NAME" = "pedidos_desnormalizado"
  }

  tags = var.tags

  depends_on = [terraform_data.bucket_gold, terraform_data.bucket_raw]
}

# ---------- Glue Data Catalog ----------
resource "aws_glue_catalog_database" "gold_db" {
  name = "prova_bigdata_gold"
}

resource "aws_glue_crawler" "gold_crawler" {
  name          = "prova-gold-crawler"
  role          = var.labrole_arn
  database_name = aws_glue_catalog_database.gold_db.name

  s3_target {
    path = "s3://${var.bucket_gold_nome}/"
  }

  tags = var.tags

  depends_on = [terraform_data.bucket_gold]
}

# ---------- Athena Workgroup ----------
resource "aws_athena_workgroup" "prova" {
  name = "prova-bigdata-wg"

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  tags = var.tags
}

# ---------- DynamoDB ----------
resource "aws_dynamodb_table" "execucoes" {
  name         = var.dynamodb_table_nome
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = var.tags
}