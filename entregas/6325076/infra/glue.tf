resource "aws_s3_object" "glue_script" {
  bucket = var.bucket_gold_nome
  key    = "glue/normaliza_pedidos.py"
  source = "${path.module}/../glue-job/normaliza_pedidos.py"
  tags   = var.tags
}
resource "aws_glue_job" "normaliza_pedidos" {
  name     = "normaliza-pedidos"
  role_arn = var.labrole_arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.bucket_gold_nome}/glue/normaliza_pedidos.py"
  }

  default_arguments = {
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"    = "execucoes"
    "--DATASET_NAME" = "pedidos_desnormalizado"
  }

  tags = var.tags
}