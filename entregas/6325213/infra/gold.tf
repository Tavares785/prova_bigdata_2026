locals {
  gold_uri = "s3://${var.bucket_gold_nome}"
}

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
    CMD
  }

  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}

resource "terraform_data" "upload_script" {
  triggers_replace = [filemd5("${path.module}/../glue-job/normaliza_pedidos.py")]

  provisioner "local-exec" {
    command = "aws s3 cp \"${path.module}/../glue-job/normaliza_pedidos.py\" \"${local.gold_uri}/scripts/normaliza_pedidos.py\""
  }

  depends_on = [terraform_data.bucket_gold]
}

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

resource "aws_glue_job" "normaliza_pedidos" {
  name              = "normaliza-pedidos"
  role_arn          = var.labrole_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 10

  command {
    name            = "glueetl"
    script_location = "${local.gold_uri}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language" = "python"
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "${local.gold_uri}/"
    "--DDB_TABLE"    = aws_dynamodb_table.execucoes.name
    "--DATASET_NAME" = "pedidos_desnormalizado"
  }

  tags = var.tags

  depends_on = [terraform_data.upload_script]
}

resource "aws_athena_workgroup" "prova" {
  name          = "prova-wg"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "${local.gold_uri}/athena-results/"
    }
  }

  tags = var.tags

  depends_on = [terraform_data.bucket_gold]
}
