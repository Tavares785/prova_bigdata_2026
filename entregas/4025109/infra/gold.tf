# gold.tf — CAMADA GOLD
# =============================================================================
# Requisitos: 4.5, 5.3, 6.6, 7.4, 8.5, 10.2, 10.3, 11.2, 11.3, 13.1
# =============================================================================

# -----------------------------------------------------------------------------
# Bloco 1 — Bucket Gold (criado via AWS CLI — PowerShell compatible)
# ⚠️ NÃO use aws_s3_bucket: a SCP do Learner Lab nega GetBucketObjectLockConfiguration.
# -----------------------------------------------------------------------------
resource "terraform_data" "bucket_gold" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }

  triggers_replace = [var.bucket_gold_nome, var.regiao]

  provisioner "local-exec" {
    interpreter = ["PowerShell", "-Command"]
    command     = <<-CMD
      aws s3api create-bucket --bucket "${var.bucket_gold_nome}" --region "${var.regiao}" 2>$null; `
      aws s3api put-public-access-block --bucket "${var.bucket_gold_nome}" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
    CMD
  }

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["PowerShell", "-Command"]
    command     = "aws s3 rb s3://${self.input.bucket} --force; exit 0"
  }
}

# -----------------------------------------------------------------------------
# Bloco 1b — Upload do script PySpark para o Bucket_Gold
# -----------------------------------------------------------------------------
resource "terraform_data" "upload_script" {
  input = {
    bucket = var.bucket_gold_nome
  }

  triggers_replace = [var.bucket_gold_nome, filemd5("${path.module}/../glue-job/normaliza_pedidos.py")]

  provisioner "local-exec" {
    interpreter = ["PowerShell", "-Command"]
    command     = "aws s3 cp '${path.module}/../glue-job/normaliza_pedidos.py' 's3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py' --content-type 'text/x-python'"
  }

  depends_on = [terraform_data.bucket_gold]
}

# -----------------------------------------------------------------------------
# Bloco 2 — Glue Job (PySpark, usa LabRole por ARN — Req 11.3)
# -----------------------------------------------------------------------------
resource "aws_glue_job" "normaliza_pedidos" {
  name     = "normaliza-pedidos"
  role_arn = var.labrole_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  default_arguments = {
    "--RAW_PATH"      = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"     = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"     = "execucoes"
    "--DATASET_NAME"  = "pedidos_desnormalizado"
    "--job-language"  = "python"
  }

  tags = var.tags

  depends_on = [terraform_data.upload_script]
}

# -----------------------------------------------------------------------------
# Bloco 3 — Glue Data Catalog Database (Req 7.4)
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_database" "prova_bigdata" {
  name        = "prova_bigdata"
  description = "Banco de dados do catálogo Glue — camada gold da prova."
}

# -----------------------------------------------------------------------------
# Bloco 3b — Glue Catalog Table: fato_pedidos (particionada por data_pedido)
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_table" "fato_pedidos" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.prova_bigdata.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"      = "parquet"
    "parquet.compression" = "SNAPPY"
    "EXTERNAL"            = "TRUE"
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
      name = "preco_unitario"
      type = "double"
    }
    columns {
      name = "quantidade"
      type = "int"
    }
    columns {
      name = "valor_total"
      type = "double"
    }
  }

  partition_keys {
    name = "data_pedido"
    type = "date"
  }

  depends_on = [aws_glue_catalog_database.prova_bigdata]
}

# -----------------------------------------------------------------------------
# Bloco 3c — Glue Catalog Table: dim_cliente
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_table" "dim_cliente" {
  name          = "dim_cliente"
  database_name = aws_glue_catalog_database.prova_bigdata.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"      = "parquet"
    "parquet.compression" = "SNAPPY"
    "EXTERNAL"            = "TRUE"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/dim_cliente/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
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

  depends_on = [aws_glue_catalog_database.prova_bigdata]
}

# -----------------------------------------------------------------------------
# Bloco 3d — Glue Catalog Table: dim_produto
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_table" "dim_produto" {
  name          = "dim_produto"
  database_name = aws_glue_catalog_database.prova_bigdata.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"      = "parquet"
    "parquet.compression" = "SNAPPY"
    "EXTERNAL"            = "TRUE"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/dim_produto/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
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

  depends_on = [aws_glue_catalog_database.prova_bigdata]
}

# -----------------------------------------------------------------------------
# Bloco 4 — Athena Workgroup (Req 7.1, 7.2)
# -----------------------------------------------------------------------------
resource "aws_athena_workgroup" "prova_bigdata" {
  name        = "prova-bigdata"
  description = "Workgroup Athena para consultas na camada gold."

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  tags = var.tags

  depends_on = [terraform_data.bucket_gold]
}

# -----------------------------------------------------------------------------
# Bloco 5 — DynamoDB (catálogo de metadados das execuções — Req 8.5, 9)
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
