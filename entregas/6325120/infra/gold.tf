# ==========================================
# CAMADA GOLD - Infraestrutura Serverless
# ==========================================

# 1. Criação do Bucket Gold via AWS CLI (Requisito da prova)
resource "terraform_data" "criar_bucket_gold" {
  provisioner "local-exec" {
    # Comando 'mb' (Make Bucket) no AWS CLI
    command = "aws s3 mb s3://${var.bucket_gold_nome} --region ${var.regiao}"
  }
}

resource "aws_glue_job" "normaliza_pedidos" {
  name     = "normaliza-pedidos"
  role_arn = var.labrole_arn          # referencia a role existente — sem aws_iam_role

  command {
    name            = "glueetl"       # identifica o runtime PySpark
    script_location = "s3://${var.bucket_raw_nome}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  default_arguments = {
    "--job-language" = "python"
    "--TempDir"      = "s3://${var.bucket_raw_nome}/tmp/"
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"    = "catalogo-execucoes"
    "--DATASET_NAME" = "pedidos_desnormalizado"
  }

  tags = var.tags
}

resource "aws_glue_catalog_database" "gold" {
  name        = "db_gold"
  description = "Banco de dados da camada Gold no Glue Data Catalog"
}

resource "aws_athena_workgroup" "principal" {
  name  = "workgroup-gold"
  state = "ENABLED"

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  tags = var.tags
}

resource "aws_dynamodb_table" "catalogo_execucoes" {
  name         = "catalogo-execucoes"
  billing_mode = "PAY_PER_REQUEST"   # serverless — cobra só por requisição
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"                       # S = String
  }

  tags = var.tags
}