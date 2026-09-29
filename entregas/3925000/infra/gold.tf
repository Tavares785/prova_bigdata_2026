# gold.tf — CAMADA GOLD
# =============================================================================
# Cria toda a infraestrutura da camada gold:
#   1. Bucket Gold (via AWS CLI, mesmo padrão do raw.tf)
#   2. Upload do script Glue para o Bucket Gold
#   3. Tabela DynamoDB para registro de execuções
#   4. Glue Job (normaliza_pedidos)
#   5. Glue Catalog Database (prova_bigdata_gold)
#   6. Glue Crawler (aponta para fato_pedidos/, dim_cliente/, dim_produto/)
#   7. Athena Workgroup (prova-bigdata)
#
# ⚠️  NÃO usa aws_s3_bucket (SCP do Academy nega GetBucketObjectLockConfiguration).
# ⚠️  NÃO cria IAM Role/Policy — usa var.labrole_arn (LabRole do Academy).
# Requirements: 3.1, 3.2, 3.4, 3.5, 3.6, 4.x, 5.x, 6.x, 7.x, 11.x
# =============================================================================

# -----------------------------------------------------------------------------
# 1. BUCKET GOLD
# Criado via AWS CLI: bucket privado + Public Access Block + tags.
# -----------------------------------------------------------------------------
resource "terraform_data" "bucket_gold" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }

  triggers_replace = [var.bucket_gold_nome, var.regiao]

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
        --tagging 'TagSet=[{Key=Projeto,Value=prova-bigdata},{Key=Ambiente,Value=lab},{Key=Responsavel,Value=3925000}]'
    CMD
  }

  # DESTROY: esvazia e remove o Bucket Gold no terraform destroy.
  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}

# -----------------------------------------------------------------------------
# 2. UPLOAD DO SCRIPT GLUE PARA O BUCKET GOLD
# Copia glue-job/normaliza_pedidos.py para s3://<gold>/scripts/
# Depende do Bucket Gold estar criado.
# -----------------------------------------------------------------------------
resource "terraform_data" "upload_script_glue" {
  input = {
    bucket = var.bucket_gold_nome
  }

  triggers_replace = [var.bucket_gold_nome]

  depends_on = [terraform_data.bucket_gold]

  provisioner "local-exec" {
    command = <<-CMD
      set -e
      aws s3 cp "${path.module}/../glue-job/normaliza_pedidos.py" \
        "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py" \
        --content-type "text/x-python"
    CMD
  }
}

# -----------------------------------------------------------------------------
# 3. DYNAMODB — tabela de registro de execuções do Glue Job
# Nome exato "execucoes" (referenciado via --DDB_TABLE=execucoes no Glue Job).
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# 4. GLUE JOB — normaliza_pedidos
# Lê do Bucket Raw, grava Parquet no Bucket Gold, registra no DynamoDB.
# Depende do upload do script e da tabela DynamoDB.
# -----------------------------------------------------------------------------
resource "aws_glue_job" "normaliza_pedidos" {
  name     = "normaliza-pedidos"
  role_arn = var.labrole_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }

  default_arguments = {
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"    = "execucoes"
    "--DATASET_NAME" = "pedidos_desnormalizado"
    "--job-language" = "python"
  }

  # Configuração econômica compatível com AWS Academy Learner Lab.
  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60

  tags = var.tags

  depends_on = [
    terraform_data.upload_script_glue,
    aws_dynamodb_table.execucoes,
  ]
}

# -----------------------------------------------------------------------------
# 5. GLUE CATALOG DATABASE — banco de dados para as tabelas gold
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_database" "gold" {
  name = "prova_bigdata_gold"

  tags = var.tags
}

# -----------------------------------------------------------------------------
# 6. GLUE CRAWLER — cataloga os Parquets gerados pelo Glue Job no Bucket Gold
# Aponta para fato_pedidos/, dim_cliente/ e dim_produto/.
# Depende do Catalog Database e do Bucket Gold.
# Obs.: a execução do crawler ocorre manualmente após o Glue Job gerar os dados.
# -----------------------------------------------------------------------------
resource "aws_glue_crawler" "gold" {
  name          = "crawler-gold"
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

  depends_on = [
    aws_glue_catalog_database.gold,
    terraform_data.bucket_gold,
  ]
}

# -----------------------------------------------------------------------------
# 7. ATHENA WORKGROUP — consultas ad-hoc sobre as tabelas gold
# Resultados gravados no próprio Bucket Gold (sem bucket separado).
# Depende do Bucket Gold.
# -----------------------------------------------------------------------------
resource "aws_athena_workgroup" "prova" {
  name = "prova-bigdata"

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  tags = var.tags

  depends_on = [terraform_data.bucket_gold]
}
