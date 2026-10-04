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
     aws s3api put-bucket-tagging \
      --bucket "${var.bucket_gold_nome}" \
      --tagging '${jsonencode({ TagSet = [for chave, valor in var.tags : { Key = chave, Value = valor }] })}'
      aws s3 cp "${path.module}/../glue-job/normaliza_pedidos.py" "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    CMD
  }
  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}
resource "aws_dynamodb_table" "metadados" {
  name         = "execucoes"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S" # tipo indicado como S (PK)
  }

  tags = var.tags
}
resource "aws_glue_catalog_database" "gold" {
  name = "prova_bigdata_gold"
  tags = var.tags
}
resource "aws_glue_crawler" "gold" {
  name          = "crawler-gold"
  role          = var.lab_role_arn
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
  depends_on = [terraform_data.bucket_gold]
  tags       = var.tags
}
resource "aws_athena_workgroup" "gold" {
  name = "athena-gold"

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }
  depends_on = [terraform_data.bucket_gold]
  tags       = var.tags
}
resource "aws_glue_job" "normaliza_pedidos" {
  name              = "normaliza-pedidos"
  role_arn          = var.lab_role_arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }
  depends_on = [terraform_data.bucket_gold]
  tags       = var.tags
}