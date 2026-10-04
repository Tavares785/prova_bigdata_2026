# gold.tf — infra (prova)
# Camada gold (escrita pelo ALUNO, RA 6325269).
# Cria o Bucket_Gold, que recebe os dados tratados pelo Glue Job,
# com acesso público bloqueado e as tags obrigatórias da prova.
# O bucket é removido junto com o resto da infra no terraform destroy.

# ---------------------------------------------------------------------------
# Caminhos
# ---------------------------------------------------------------------------
locals {
  script_origem   = "${path.module}/../glue-job/normaliza_pedidos.py"
  script_s3       = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
  raw_path        = "s3://${var.bucket_raw_nome}/pedidos/"
  gold_path       = "s3://${var.bucket_gold_nome}/"
  loc_fato        = "${local.gold_path}fato_pedidos/"
  loc_dim_cliente = "${local.gold_path}dim_cliente/"
  loc_dim_produto = "${local.gold_path}dim_produto/"
  athena_results  = "${local.gold_path}athena-results/"
}

# ---------------------------------------------------------------------------
# Bucket gold
# ---------------------------------------------------------------------------
resource "terraform_data" "bucket_gold" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }

  triggers_replace = [var.bucket_gold_nome, var.regiao]

  provisioner "local-exec" {
    command = <<-CMD
      set -e
      aws s3api head-bucket --bucket "${var.bucket_gold_nome}" 2>/dev/null || aws s3api create-bucket --bucket "${var.bucket_gold_nome}" --region "${var.regiao}"
      aws s3api put-public-access-block --bucket "${var.bucket_gold_nome}" --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
      aws s3api put-bucket-tagging --bucket "${var.bucket_gold_nome}" --tagging '${jsonencode({ TagSet = [for k, v in var.tags : { Key = k, Value = v }] })}' || echo "AVISO: nao foi possivel aplicar as tags no bucket ${var.bucket_gold_nome}; seguindo sem elas."
    CMD
  }

  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb \"s3://${self.input.bucket}\" --force || true"
  }
}

# ---------------------------------------------------------------------------
# Script do Glue Job
# ---------------------------------------------------------------------------
resource "terraform_data" "script_glue" {
  input = {
    script_s3 = local.script_s3
  }

  triggers_replace = [filemd5(local.script_origem), var.bucket_gold_nome]

  depends_on = [terraform_data.bucket_gold]

  provisioner "local-exec" {
    command = <<-CMD
      set -e
      aws s3 cp "${local.script_origem}" "${local.script_s3}"
    CMD
  }
}

# ---------------------------------------------------------------------------
# Catálogo de execuções (DynamoDB)
# ---------------------------------------------------------------------------
resource "aws_dynamodb_table" "execucoes" {
  name         = var.dynamodb_tabela_nome
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = var.tags
}

# ---------------------------------------------------------------------------
# Glue Job (normalização raw -> gold)
# ---------------------------------------------------------------------------
resource "aws_glue_job" "normaliza_pedidos" {
  name              = var.glue_job_nome
  role_arn          = var.labrole_arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 10
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = local.script_s3
    python_version  = "3"
  }

  default_arguments = {
    "--RAW_PATH"            = local.raw_path
    "--GOLD_PATH"           = local.gold_path
    "--DDB_TABLE"           = aws_dynamodb_table.execucoes.name
    "--DATASET_NAME"        = var.dataset_nome
    "--job-bookmark-option" = "job-bookmark-disable"
    "--job-language"        = "python"
  }

  tags = var.tags
}

# ---------------------------------------------------------------------------
# Glue Data Catalog
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_database" "gold" {
  name = var.glue_database_nome
  tags = var.tags
}

resource "aws_glue_catalog_table" "fato_pedidos" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"                       = "parquet"
    "EXTERNAL"                             = "TRUE"
    "projection.enabled"                   = "true"
    "projection.data_pedido.type"          = "date"
    "projection.data_pedido.range"         = "2026-01-01,NOW"
    "projection.data_pedido.format"        = "yyyy-MM-dd"
    "projection.data_pedido.interval"      = "1"
    "projection.data_pedido.interval.unit" = "DAYS"
    "storage.location.template"            = "${local.loc_fato}data_pedido=$${data_pedido}/"
  }

  partition_keys {
    name = "data_pedido"
    type = "date"
  }

  storage_descriptor {
    location      = local.loc_fato
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "pedido_id"
      type = "string"
    }

    columns {
      name = "cliente_id"
      type = "string"
    }

    columns {
      name = "produto_id"
      type = "string"
    }

    columns {
      name = "quantidade"
      type = "int"
    }

    columns {
      name = "preco_unitario"
      type = "double"
    }

    columns {
      name = "valor_total"
      type = "double"
    }
  }
}

resource "aws_glue_catalog_table" "dim_cliente" {
  name          = "dim_cliente"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
    "EXTERNAL"       = "TRUE"
  }

  storage_descriptor {
    location      = local.loc_dim_cliente
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "cliente_id"
      type = "string"
    }

    columns {
      name = "cliente_nome"
      type = "string"
    }

    columns {
      name = "cliente_uf"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "dim_produto" {
  name          = "dim_produto"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
    "EXTERNAL"       = "TRUE"
  }

  storage_descriptor {
    location      = local.loc_dim_produto
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "produto_id"
      type = "string"
    }

    columns {
      name = "produto_nome"
      type = "string"
    }

    columns {
      name = "categoria"
      type = "string"
    }
  }
}

# ---------------------------------------------------------------------------
# Athena
# ---------------------------------------------------------------------------
resource "aws_athena_workgroup" "gold" {
  name          = var.athena_workgroup_nome
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = 10485760

    result_configuration {
      output_location = local.athena_results

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags       = var.tags
  depends_on = [terraform_data.bucket_gold]
}
