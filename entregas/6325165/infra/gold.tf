resource "terraform_data" "bucket_gold" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }

  triggers_replace = [var.bucket_gold_nome, var.regiao]

  provisioner "local-exec" {
    command     = <<-CMD
      set -e
      aws s3api create-bucket --bucket "${var.bucket_gold_nome}" --region "${var.regiao}" 2>/dev/null || true
      aws s3api put-public-access-block --bucket "${var.bucket_gold_nome}" \
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
    CMD
    interpreter = ["bash", "-c"]
  }

  provisioner "local-exec" {
    when        = destroy
    command     = "aws s3 rb s3://${self.input.bucket} --force || true"
    interpreter = ["bash", "-c"]
  }
}

resource "null_resource" "upload_glue_script" {
  depends_on = [terraform_data.bucket_gold]

  triggers = {
    script_hash = filemd5("${path.module}/../glue-job/normaliza_pedidos.py")
  }

  provisioner "local-exec" {
    command     = "aws s3 cp ${path.module}/../glue-job/normaliza_pedidos.py s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    interpreter = ["bash", "-c"]
  }
}

resource "aws_glue_job" "normaliza_pedidos" {
  name     = "normaliza-pedidos"
  role_arn = var.labrole_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }

  default_arguments = {
    "--RAW_PATH"        = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"       = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"       = "execucoes_normalizacao"
    "--DATASET_NAME"    = "pedidos_desnormalizado"
    "--job-language"    = "python"
    "--enable-metrics"  = "true"
  }

  glue_version       = "4.0"
  worker_type        = "G.1X"
  number_of_workers = 2
  timeout           = 10

  tags = var.tags

  depends_on = [null_resource.upload_glue_script]
}

resource "aws_glue_catalog_database" "gold_db" {
  name = "db_gold_pedidos"
}

resource "aws_glue_crawler" "gold_crawler" {
  database_name = aws_glue_catalog_database.gold_db.name
  name          = "crawler-gold-pedidos"
  role          = var.labrole_arn

  s3_target {
    path = "s3://${var.bucket_gold_nome}/"
  }

  tags = var.tags

  depends_on = [terraform_data.bucket_gold]
}

resource "aws_athena_workgroup" "gold_workgroup" {
  name = "wg-gold-pedidos"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  tags = var.tags

  depends_on = [terraform_data.bucket_gold]
}

resource "aws_dynamodb_table" "execucoes" {
  name         = "execucoes_normalizacao"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = var.tags
}