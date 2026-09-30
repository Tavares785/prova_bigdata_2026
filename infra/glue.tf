resource "aws_s3_object" "glue_script" {
  bucket = var.bucket_raw_nome
  key    = "scripts/normaliza_pedidos.py"
  source = "../glue-job/normaliza_pedidos.py"
  etag   = filemd5("../glue-job/normaliza_pedidos.py")
}

resource "aws_glue_job" "prova_bigdata_glue_job" {
  name     = "prova_bigdata_glue_job"
  role_arn = var.lab_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_raw_nome}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language" = "python"
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/pedidos_desnormalizado.csv"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/pedidos/"
    "--DDB_TABLE"    = "pedidos_gold"
    "--DATASET_NAME" = "pedidos"
  }

  depends_on = [aws_s3_object.glue_script]
}
