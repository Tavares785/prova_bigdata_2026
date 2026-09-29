resource "aws_glue_job" "normaliza_pedidos" {
  name     = "normaliza-pedidos"
  role_arn = var.labrole_arn

  depends_on = [
    terraform_data.upload_glue_script
  ]

  # Configuração do script PySpark
  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }

  glue_version = "4.0"

  worker_type       = "G.1X"
  number_of_workers = 2

  # Argumentos que o script Python vai ler
  default_arguments = {
    "--RAW_PATH"  = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH" = "s3://${var.bucket_gold_nome}/pedidos_normalizados/"
    "--DDB_TABLE" = aws_dynamodb_table.tabela_execucoes.name

    "--DATASET_NAME" = "pedidos_desnormalizado"
  }

  tags = var.tags
}