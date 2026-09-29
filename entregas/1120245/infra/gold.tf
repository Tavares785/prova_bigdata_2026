# gold.tf — CAMADA GOLD  ✍️ ALUNO (construída por Matheus Mantovani — RA 1120245)
# =============================================================================
# Provisiona toda a infraestrutura da camada gold:
#   1. Bucket_Gold (privado, via AWS CLI — mesmo padrão do raw.tf)
#   2. Upload do script PySpark para o Bucket_Gold
#   3. AWS Glue Job (glueetl, PySpark, usa LabRole por ARN)
#   4. Glue Data Catalog — database + tabelas (fato_pedidos, dim_cliente, dim_produto)
#   4b. Glue Crawler — descobre partições após o Job e atualiza o Catalog
#   5. Athena Workgroup com output no Bucket_Gold
#   6. DynamoDB — tabela de metadados de execução (PAY_PER_REQUEST)
# Requirements: 4.1, 4.2, 4.3, 5.1, 5.2, 7.1, 7.4, 8.1, 8.2, 10.3, 11.2, 11.3, 13.1
# =============================================================================

# -----------------------------------------------------------------------------
# 1. Bucket_Gold — criado via AWS CLI (mesmo padrão do raw.tf).
#    NÃO usa aws_s3_bucket: a SCP do Learner Lab nega GetBucketObjectLockConfiguration.
# -----------------------------------------------------------------------------
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

  # DESTROY: esvazia e remove o Bucket_Gold no terraform destroy.
  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}

# -----------------------------------------------------------------------------
# 2. Upload do script PySpark para o Bucket_Gold.
#    O Glue Job referencia esse caminho como script_location.
#    Depende do bucket gold existir antes do upload.
# -----------------------------------------------------------------------------
resource "terraform_data" "upload_script_glue" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }

  triggers_replace = [var.bucket_gold_nome]

  depends_on = [terraform_data.bucket_gold]

  provisioner "local-exec" {
    command = <<-CMD
      set -e
      aws s3 cp "${path.module}/../glue-job/normaliza_pedidos.py" \
        "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    CMD
  }
}

# -----------------------------------------------------------------------------
# 3. AWS Glue Job — motor PySpark serverless.
#    role_arn aponta para a LabRole (sem criar roles/policies próprias).
#    Argumentos padrão passam os caminhos S3 e a tabela DynamoDB para o script.
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
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 10 # minutos — mantém custo baixo no Learner Lab

  default_arguments = {
    "--RAW_PATH"       = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"      = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"      = "execucoes-1120245"
    "--DATASET_NAME"   = "pedidos_desnormalizado"
    "--job-language"   = "python"
    "--enable-metrics" = "true"
  }

  depends_on = [terraform_data.upload_script_glue]

  tags = var.tags
}

# -----------------------------------------------------------------------------
# 4. Glue Data Catalog — database + 3 tabelas (fato_pedidos, dim_cliente, dim_produto).
#    Permite que o Athena consulte os Parquet do Bucket_Gold via SQL.
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_database" "gold_db" {
  name = "gold_pedidos_1120245"

  tags = var.tags
}

resource "aws_glue_catalog_table" "fato_pedidos" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.gold_db.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
    "EXTERNAL"       = "TRUE"
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

  # Partição por data_pedido — reflete o layout do Bucket_Gold.
  partition_keys {
    name = "data_pedido"
    type = "date"
  }

  depends_on = [terraform_data.bucket_gold]
}

resource "aws_glue_catalog_table" "dim_cliente" {
  name          = "dim_cliente"
  database_name = aws_glue_catalog_database.gold_db.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
    "EXTERNAL"       = "TRUE"
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

  depends_on = [terraform_data.bucket_gold]
}

resource "aws_glue_catalog_table" "dim_produto" {
  name          = "dim_produto"
  database_name = aws_glue_catalog_database.gold_db.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
    "EXTERNAL"       = "TRUE"
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

  depends_on = [terraform_data.bucket_gold]
}


# -----------------------------------------------------------------------------
# 4b. Glue Crawler — descobre partições do fato_pedidos e atualiza o Catalog.
#     Equivale ao MSCK REPAIR TABLE, mas executado de forma declarativa via
#     Terraform. Usa a LabRole (sem criar roles/policies próprias).
#     Execute manualmente após o Glue Job: aws glue start-crawler --name <nome>
# -----------------------------------------------------------------------------
resource "aws_glue_crawler" "gold_crawler" {
  name          = "gold-crawler-1120245"
  role          = var.labrole_arn
  database_name = aws_glue_catalog_database.gold_db.name

  s3_target {
    path = "s3://${var.bucket_gold_nome}/fato_pedidos/"
  }

  s3_target {
    path = "s3://${var.bucket_gold_nome}/dim_cliente/"
  }

  s3_target {
    path = "s3://${var.bucket_gold_nome}/dim_produto/"
  }

  # Sobrescreve as tabelas no Catalog caso o schema mude entre execuções.
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  depends_on = [
    aws_glue_catalog_database.gold_db,
    terraform_data.bucket_gold,
  ]

  tags = var.tags
}

# -----------------------------------------------------------------------------
# 5. Athena Workgroup — local de resultados no Bucket_Gold.
# -----------------------------------------------------------------------------
resource "aws_athena_workgroup" "gold_wg" {
  name          = "gold-wg-1120245"
  force_destroy = true

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  depends_on = [terraform_data.bucket_gold]

  tags = var.tags
}

# -----------------------------------------------------------------------------
# 6. DynamoDB — tabela de metadados de execução (Req 8.1, 8.2).
#    Chave de partição: execution_id (string). PAY_PER_REQUEST = sem custo residual.
# -----------------------------------------------------------------------------
resource "aws_dynamodb_table" "execucoes" {
  name         = "execucoes-1120245"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = var.tags
}
