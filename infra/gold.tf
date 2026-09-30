# =============================================================================
# 1. Bucket_Gold — criado via AWS CLI (terraform_data + local-exec).
# =============================================================================
resource "terraform_data" "bucket_gold" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }

  triggers_replace = [var.bucket_gold_nome, var.regiao]

  provisioner "local-exec" {
    command = <<-CMD
      set -e
      aws s3api create-bucket \
        --bucket "${var.bucket_gold_nome}" \
        --region "${var.regiao}" 2>/dev/null || true
      aws s3 cp "${path.module}/../glue-job/normaliza_pedidos.py" \
        "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    CMD
  }

  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}

# =============================================================================
# 2. AWS Glue Job — normaliza-pedidos
# =============================================================================
resource "aws_glue_job" "normaliza_pedidos" {
  name     = "normaliza-pedidos"
  role_arn = var.labrole_arn
  tags     = var.tags

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
  }

  default_arguments = {
    # Caminhos do pipeline
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"    = var.dynamodb_tabela
    "--DATASET_NAME" = "pedidos_desnormalizado"

    # Logs contínuos do driver → CloudWatch (/aws-glue/jobs/output)
    "--enable-continuous-cloudwatch-log" = "true"

    # Diretório temporário do Glue (obrigatório para alguns conectores)
    "--TempDir" = "s3://${var.bucket_gold_nome}/tmp/"

    # Habilita Glue Data Catalog como metastore do Spark
    "--enable-glue-datacatalog" = "true"
  }

  depends_on = [terraform_data.bucket_gold]
}

# =============================================================================
# 3. Glue Data Catalog — banco de dados + 3 tabelas Parquet
# =============================================================================

resource "aws_glue_catalog_database" "gold_db" {
  name        = "gold_pedidos"
  description = "Banco de dados do catálogo Glue — camada gold da prova (modelo estrela)."
}

# --- Tabela: fato_pedidos (particionada por data_pedido) ---------------------
resource "aws_glue_catalog_table" "fato_pedidos" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.gold_db.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"      = "parquet"
    "parquet.compression" = "SNAPPY"
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

    # Colunas do fato — data_pedido é partition_key (não entra aqui).
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

  # Particionamento por data_pedido (Parquet partitioned by date).
  partition_keys {
    name = "data_pedido"
    type = "date"
  }

  depends_on = [aws_glue_catalog_database.gold_db]
}

# --- Tabela: dim_cliente (sem partição) --------------------------------------
resource "aws_glue_catalog_table" "dim_cliente" {
  name          = "dim_cliente"
  database_name = aws_glue_catalog_database.gold_db.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"      = "parquet"
    "parquet.compression" = "SNAPPY"
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

  depends_on = [aws_glue_catalog_database.gold_db]
}

# --- Tabela: dim_produto (sem partição) --------------------------------------
resource "aws_glue_catalog_table" "dim_produto" {
  name          = "dim_produto"
  database_name = aws_glue_catalog_database.gold_db.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"      = "parquet"
    "parquet.compression" = "SNAPPY"
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

  depends_on = [aws_glue_catalog_database.gold_db]
}

# =============================================================================
# 4. Athena Workgroup — gold-workgroup
# =============================================================================
resource "aws_athena_workgroup" "gold_wg" {
  name  = "gold-workgroup"
  state = "ENABLED"
  tags  = var.tags

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false

    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  depends_on = [terraform_data.bucket_gold]
}

# =============================================================================
# 5. DynamoDB — tabela de metadados de execução
# =============================================================================
resource "aws_dynamodb_table" "execucoes" {
  name         = var.dynamodb_tabela
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"
  tags         = var.tags

  attribute {
    name = "execution_id"
    type = "S"
  }
}