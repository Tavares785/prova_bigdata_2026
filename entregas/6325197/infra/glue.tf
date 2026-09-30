resource "terraform_data" "script_glue" {
  triggers_replace = [
    filesha256("${path.module}/../glue-job/normaliza_pedidos.py"),
    var.bucket_gold_nome
  ]

  depends_on = [terraform_data.bucket_gold]

  provisioner "local-exec" {
    environment = {
      SCRIPT_PATH = "${path.module}/../glue-job/normaliza_pedidos.py"
      GOLD_BUCKET = var.bucket_gold_nome
      AWS_REGION  = var.regiao
    }

    command = "aws s3 cp \"$SCRIPT_PATH\" \"s3://$GOLD_BUCKET/scripts/normaliza_pedidos.py\" --region \"$AWS_REGION\""
  }
}

resource "aws_glue_job" "normaliza_pedidos" {
  name              = "normaliza-pedidos-6325197"
  role_arn          = var.labrole_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 10
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
    "--DATASET_NAME" = "pedidos_desnormalizado"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags

  depends_on = [
    terraform_data.script_glue,
    terraform_data.bucket_raw
  ]
}

output "glue_job_nome" {
  value = aws_glue_job.normaliza_pedidos.name
}
