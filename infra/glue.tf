# glue.tf — Glue Job de normalização (LabRole por ARN; sem criar roles/policies).
# Custo: Glue não tem free tier (cobra DPU-hora) -> mínimo de workers, timeout curto e sem retry.

resource "aws_glue_job" "normaliza" {
  name              = var.glue_job_nome
  role_arn          = var.labrole_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 10 # minutos
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
  }

  default_arguments = {
    "--job-language" = "python"
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"    = aws_dynamodb_table.execucoes.name
    "--DATASET_NAME" = var.dataset_nome
  }

  tags = var.tags

  depends_on = [terraform_data.upload_script, terraform_data.bucket_raw]
}
