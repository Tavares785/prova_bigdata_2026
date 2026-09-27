# gold.tf — CAMADA GOLD ✍️ ALUNO CONSTRÓI
# =============================================================================
# Camada gold da prova: Bucket_Gold, upload do script PySpark, Glue Data
# Catalog (+ Crawler), Glue Job (glueetl) usando a LabRole por ARN, Athena
# Workgroup e a tabela DynamoDB de metadados das execuções.
#
# ⚠️ Learner Lab: assim como o Bucket_Raw (raw.tf), o Bucket_Gold NÃO usa o
# recurso aws_s3_bucket (a SCP da organização nega a leitura de configuração
# de object lock que esse recurso sempre executa -> AccessDenied). Por isso é
# criado via AWS CLI dentro do próprio `terraform apply` (terraform_data +
# local-exec), no mesmo padrão do raw.tf.
# Requirements: 4.x, 6.x, 7.x, 8.x, 9.x, 10.3, 11.x.
# =============================================================================

# -----------------------------------------------------------------------------
# Bucket_Gold — criado via AWS CLI, privado, com tags de custo.
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
      aws s3api create-bucket --bucket "${var.bucket_gold_nome}" --region "${var.regiao}" 2>/dev/null || true
      aws s3api put-public-access-block --bucket "${var.bucket_gold_nome}" \
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
      aws s3api put-bucket-tagging --bucket "${var.bucket_gold_nome}" --tagging \
        'TagSet=[{Key=Projeto,Value=${var.tags["Projeto"]}},{Key=Disciplina,Value=${var.tags["Disciplina"]}},{Key=Ambiente,Value=${var.tags["Ambiente"]}}]'
    CMD
  }

  # DESTROY: esvazia e remove o Bucket_Gold no terraform destroy.
  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}

# -----------------------------------------------------------------------------
# Upload do script PySpark do Glue Job para o Bucket_Gold.
# Re-executa sempre que o conteúdo do script mudar (triggers_replace por hash).
# -----------------------------------------------------------------------------
resource "terraform_data" "upload_script" {
  depends_on = [terraform_data.bucket_gold]

  input = {
    bucket = var.bucket_gold_nome
  }

  triggers_replace = [filemd5("${path.module}/../glue-job/normaliza_pedidos.py")]

  provisioner "local-exec" {
    command = <<-CMD
      set -e
      aws s3 cp "${path.module}/../glue-job/normaliza_pedidos.py" \
        "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py" --content-type text/x-python
    CMD
  }
}

# -----------------------------------------------------------------------------
# LabRole — referenciada por ARN via var.labrole_arn (sem criar roles/policies
# próprias, conforme exigido pelo Learner Lab).
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Glue Data Catalog — database da camada gold.
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_database" "gold" {
  name = "prova_bigdata_gold"
}

# -----------------------------------------------------------------------------
# Glue Crawler — descobre fato_pedidos/dim_cliente/dim_produto (e as partições
# de data_pedido) a partir dos arquivos Parquet gravados pelo Glue Job no
# Bucket_Gold. Disparado manualmente após o Job (aws glue start-crawler),
# antes de consultar no Athena.
# -----------------------------------------------------------------------------
resource "aws_glue_crawler" "gold" {
  name          = "prova-bigdata-gold-crawler"
  role          = var.labrole_arn
  database_name = aws_glue_catalog_database.gold.name
  tags          = var.tags

  s3_target {
    path       = "s3://${var.bucket_gold_nome}/"
    exclusions = ["scripts/**", "athena-results/**"]
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior  = "UPDATE_IN_DATABASE"
  }

  depends_on = [terraform_data.bucket_gold]
}

# -----------------------------------------------------------------------------
# Glue Job (glueetl / PySpark) — normaliza_pedidos.py, usando a LabRole.
# Worker econômico (G.1X, 2 workers) e timeout curto para controlar custo.
# -----------------------------------------------------------------------------
resource "aws_glue_job" "normaliza_pedidos" {
  name              = "normaliza-pedidos"
  role_arn          = var.labrole_arn
  glue_version      = "5.0" # Spark 3.5.x — coerente com o PySpark 3.5.1 do local-test.
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30
  max_retries       = 0
  tags              = var.tags

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }

  default_arguments = {
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"    = aws_dynamodb_table.execucoes.name
    "--DATASET_NAME" = "pedidos_desnormalizado"
    "--job-language" = "python"
  }

  depends_on = [terraform_data.upload_script]
}

# -----------------------------------------------------------------------------
# Athena Workgroup — local de resultados de consulta (query results location).
# -----------------------------------------------------------------------------
resource "aws_athena_workgroup" "prova" {
  name          = "prova-bigdata-wg"
  force_destroy = true # permite terraform destroy mesmo com histórico de consultas.
  tags          = var.tags

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  depends_on = [terraform_data.bucket_gold]
}

# -----------------------------------------------------------------------------
# DynamoDB — catálogo de metadados das execuções (PAY_PER_REQUEST).
# -----------------------------------------------------------------------------
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
