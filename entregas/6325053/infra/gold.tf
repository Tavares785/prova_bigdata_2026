data "aws_caller_identity" "atual" {}

locals {
  prefixo  = "prova-bigdata-6325053"
  database = "prova_bigdata_6325053"
}

resource "terraform_data" "bucket_gold" {
  input = {
    bucket  = var.bucket_gold_nome
    account = data.aws_caller_identity.atual.account_id
    helper  = abspath("${path.module}/bucket_cli.py")
  }

  triggers_replace = [var.bucket_gold_nome]

  lifecycle {
    precondition {
      condition     = var.bucket_gold_nome != var.bucket_raw_nome
      error_message = "Raw e gold devem usar buckets diferentes."
    }

    precondition {
      condition     = split(":", var.labrole_arn)[4] == data.aws_caller_identity.atual.account_id
      error_message = "A LabRole deve pertencer à conta autenticada."
    }

    precondition {
      condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.bucket_raw_nome))
      error_message = "Use nome raw minúsculo, com números e hífens, de 3 a 63 caracteres."
    }
  }

  provisioner "local-exec" {
    command = "python3 \"${path.module}/bucket_cli.py\" create"

    environment = {
      BUCKET_NAME = var.bucket_gold_nome
      ACCOUNT_ID  = data.aws_caller_identity.atual.account_id
      BUCKET_TAGS = jsonencode(var.tags)
    }
  }

  provisioner "local-exec" {
    when    = destroy
    command = "python3 \"${self.input.helper}\" delete"

    environment = {
      BUCKET_NAME = self.input.bucket
      ACCOUNT_ID  = self.input.account
    }
  }
}

resource "terraform_data" "tags_buckets" {
  for_each = toset(["raw", "gold"])

  triggers_replace = [
    var.tags,
    var.bucket_raw_nome,
    var.bucket_gold_nome,
    terraform_data.bucket_raw.id,
    terraform_data.bucket_gold.id
  ]

  provisioner "local-exec" {
    command = "python3 \"${path.module}/bucket_cli.py\" tag"

    environment = {
      BUCKET_NAME = each.key == "raw" ? var.bucket_raw_nome : var.bucket_gold_nome
      ACCOUNT_ID  = data.aws_caller_identity.atual.account_id
      BUCKET_TAGS = jsonencode(var.tags)
    }
  }

  depends_on = [
    terraform_data.bucket_raw,
    terraform_data.bucket_gold
  ]
}

resource "terraform_data" "script_glue" {
  triggers_replace = [
    filesha256("${path.module}/../glue-job/normaliza_pedidos.py"),
    terraform_data.bucket_gold.id
  ]

  provisioner "local-exec" {
    command = "aws s3 cp \"${path.module}/../glue-job/normaliza_pedidos.py\" \"s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py\" --region us-east-1 --no-cli-pager"
  }

  depends_on = [terraform_data.tags_buckets]
}

resource "aws_dynamodb_table" "execucoes" {
  name         = "${local.prefixo}-execucoes"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = var.tags
}

resource "aws_glue_job" "normaliza" {
  name              = "${local.prefixo}-normaliza"
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

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--job-language"        = "python"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--RAW_PATH"            = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"           = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"           = aws_dynamodb_table.execucoes.name
    "--DATASET_NAME"        = "pedidos_desnormalizado"
    "--TempDir"             = "s3://${var.bucket_gold_nome}/temp/"
  }

  tags = var.tags

  depends_on = [
    terraform_data.script_glue,
    terraform_data.bucket_raw
  ]
}

resource "aws_glue_catalog_database" "gold" {
  name = local.database
  tags = var.tags
}

resource "aws_glue_catalog_table" "dimensoes" {
  for_each = {
    dim_cliente = ["cliente_id", "cliente_nome", "cliente_uf"]
    dim_produto = ["produto_id", "produto_nome", "categoria"]
  }

  name          = each.key
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
    EXTERNAL       = "TRUE"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/${each.key}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = each.value

      content {
        name = columns.value
        type = "string"
      }
    }
  }

  depends_on = [terraform_data.bucket_gold]
}

resource "aws_glue_catalog_table" "fato" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
    EXTERNAL       = "TRUE"
  }

  partition_keys {
    name = "data_pedido"
    type = "date"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/fato_pedidos/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = [
        { name = "pedido_id", type = "string" },
        { name = "cliente_id", type = "string" },
        { name = "produto_id", type = "string" },
        { name = "preco_unitario", type = "double" },
        { name = "quantidade", type = "int" },
        { name = "valor_total", type = "double" }
      ]

      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }

  depends_on = [terraform_data.bucket_gold]
}

resource "aws_athena_workgroup" "gold" {
  name          = local.prefixo
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    bytes_scanned_cutoff_per_query     = 10485760

    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = var.tags

  depends_on = [terraform_data.bucket_gold]
}
