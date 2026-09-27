# Camada Gold: armazenamento, processamento, catálogo e metadados.
# Os buckets são criados via AWS CLI porque o Learner Lab bloqueia o refresh
# de aws_s3_bucket por causa da configuração de Object Lock.

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
    CMD
  }

  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}

resource "terraform_data" "glue_script" {
  input = {
    bucket = var.bucket_gold_nome
    script = "${path.module}/../glue-job/normaliza_pedidos.py"
  }

  triggers_replace = [filesha256("${path.module}/../glue-job/normaliza_pedidos.py")]

  provisioner "local-exec" {
    command = <<-CMD
      set -e
      aws s3 cp "${path.module}/../glue-job/normaliza_pedidos.py" \
        "s3://${var.bucket_gold_nome}/jobs/normaliza_pedidos.py" \
        --content-type text/x-python
    CMD
  }

  depends_on = [terraform_data.bucket_gold]
}

resource "aws_glue_job" "normalizacao" {
  name              = "normaliza-pedidos"
  role_arn          = var.labrole_arn
  glue_version      = "4.0"
  description       = "Normaliza pedidos do Bucket Raw para o Bucket Gold."
  max_retries       = 0
  timeout           = 15
  number_of_workers = 2
  worker_type       = "G.1X"

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.bucket_gold_nome}/jobs/normaliza_pedidos.py"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--RAW_PATH"                         = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"                        = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"                        = aws_dynamodb_table.execucoes.name
    "--DATASET_NAME"                     = "pedidos_desnormalizado"
  }

  tags       = var.tags
  depends_on = [terraform_data.glue_script]
}

resource "aws_glue_catalog_database" "gold" {
  name        = "prova_bigdata_gold"
  description = "Catalogo das tabelas normalizadas do Bucket Gold."
}

resource "aws_glue_crawler" "gold" {
  name          = "cataloga-gold-pedidos"
  role          = var.labrole_arn
  database_name = aws_glue_catalog_database.gold.name
  description   = "Cataloga fato_pedidos e dimensoes no Bucket Gold."
  table_prefix  = ""

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

  tags       = var.tags
  depends_on = [aws_glue_catalog_database.gold, aws_glue_job.normalizacao]
}

resource "aws_athena_workgroup" "gold" {
  name          = "prova-bigdata-gold"
  description   = "Consultas Athena do modelo dimensional Gold."
  force_destroy = true

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  tags = var.tags
}

resource "aws_dynamodb_table" "execucoes" {
  name         = "execucoes"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = var.tags
}
