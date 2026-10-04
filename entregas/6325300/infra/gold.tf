# gold.tf — camada gold da prova
# Recursos construídos pelo aluno: bucket gold, Glue, Athena, Glue Catalog e DynamoDB.

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
resource "terraform_data" "upload_glue_script" {
  input = {
    bucket = var.bucket_gold_nome
    script = "${path.module}/../glue-job/normaliza_pedidos.py"
  }

  triggers_replace = [
    filesha256("${path.module}/../glue-job/normaliza_pedidos.py")
  ]

  depends_on = [terraform_data.bucket_gold]

  provisioner "local-exec" {
    command = <<-CMD
      set -e
      aws s3 cp "${self.input.script}" "s3://${self.input.bucket}/scripts/normaliza_pedidos.py" --content-type text/x-python
    CMD
  }
}
resource "aws_glue_job" "normaliza_pedidos" {
  name     = "normaliza-pedidos"
  role_arn = var.labrole_arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 10

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }

  default_arguments = {
    "--RAW_PATH"       = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"      = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"      = aws_dynamodb_table.execucoes.name
    "--DATASET_NAME"   = "pedidos_desnormalizado"
    "--job-language"   = "python"
    "--enable-metrics" = "true"
  }

  depends_on = [
    terraform_data.bucket_raw,
    terraform_data.bucket_gold,
    terraform_data.upload_glue_script,
    aws_dynamodb_table.execucoes,
  ]

  tags = var.tags
}
resource "aws_glue_catalog_database" "gold" {
  name = "prova_bigdata_6325300"
}
resource "aws_glue_crawler" "gold" {
  name          = "crawler-gold-pedidos"
  role          = var.labrole_arn
  database_name = aws_glue_catalog_database.gold.name

  s3_target {
    path = "s3://${var.bucket_gold_nome}/"
  }

  depends_on = [
    terraform_data.bucket_gold,
    aws_glue_catalog_database.gold,
  ]

  tags = var.tags
}
resource "aws_athena_workgroup" "prova" {
  name = "prova-bigdata-6325300"

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  tags = var.tags
}