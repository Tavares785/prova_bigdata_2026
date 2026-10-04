data "aws_caller_identity" "current" {}

# Faz o upload do script Python para o Bucket Gold
resource "aws_s3_object" "glue_script" {
  bucket = var.bucket_gold_nome
  key    = "scripts/normaliza_pedidos.py"
  source = "${path.module}/../glue-job/normaliza_pedidos.py"
  etag   = filemd5("${path.module}/../glue-job/normaliza_pedidos.py")
  tags   = var.tags

  depends_on = [terraform_data.bucket_gold]
}

# Tabela DynamoDB para Metadados
resource "aws_dynamodb_table" "metadados" {
  name         = "BigData_Metadados_Execucao"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = var.tags
}

# AWS Glue Job de Normalização
resource "aws_glue_job" "normalizacao" {
  name     = "Job_Normaliza_Pedidos"
  role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/LabRole"

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language" = "python"
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"    = aws_dynamodb_table.metadados.name
    "--DATASET_NAME" = "pedidos_desnormalizado"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 15

  tags = var.tags
}
