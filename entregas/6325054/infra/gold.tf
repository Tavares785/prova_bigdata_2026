# gold.tf — CAMADA GOLD (entrega 6325054)
# =============================================================================
# Provisiona toda a camada gold da prova:
#   - Bucket_Gold (privado, via AWS CLI — mesmo padrão do raw.tf)
#   - Upload do script PySpark para o S3 (script_location do Glue Job)
#   - Referência à LabRole por ARN (sem criar roles/policies próprias)
#   - Glue Job PySpark (glueetl)
#   - Glue Data Catalog: database + tabelas fato_pedidos, dim_cliente, dim_produto
#   - Athena Workgroup com output location no próprio bucket gold
#   - DynamoDB (PAY_PER_REQUEST, hash_key = execution_id)
#
# ⚠️ Learner Lab: NÃO usa aws_s3_bucket (SCP bloqueia GetBucketObjectLockConfiguration).
#    Buckets criados via AWS CLI (terraform_data + local-exec). Requer AWS CLI v2.
# Requirements: 4.1–4.6, 5.1–5.3, 7.1–7.4, 8.1–8.5, 10.3, 11.2, 11.3
# =============================================================================

# ---------------------------------------------------------------------------
# 1. Bucket_Gold — criado via AWS CLI (mesmo padrão do Bucket_Raw)
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

# ---------------------------------------------------------------------------
# 2. Upload do script PySpark para o Bucket_Gold
#    O Glue Job lê o script diretamente do S3 (script_location).
# ---------------------------------------------------------------------------

resource "terraform_data" "upload_script_glue" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }

  triggers_replace = [var.bucket_gold_nome, filemd5("${path.module}/../glue-job/normaliza_pedido.py")]

  provisioner "local-exec" {
    command = <<-CMD
      set -e
      aws s3 cp "${path.module}/../glue-job/normaliza_pedido.py" \
        "s3://${var.bucket_gold_nome}/scripts/normaliza_pedido.py"
    CMD
  }

  depends_on = [terraform_data.bucket_gold]
}

# ---------------------------------------------------------------------------
# 3. Referência à LabRole por ARN (sem criar role própria — Req 11.3)
# ---------------------------------------------------------------------------

data "aws_iam_role" "lab_role" {
  name = "LabRole"
}

# ---------------------------------------------------------------------------
# 4. Glue Job PySpark (glueetl)
# ---------------------------------------------------------------------------

resource "aws_glue_job" "normaliza_pedidos" {
  name     = var.glue_job_nome
  role_arn = data.aws_iam_role.lab_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedido.py"
    python_version  = "3"
  }

  # Versão 4.0 do Glue = Spark 3.3 + Python 3.10 — boa compatibilidade e custo razoável.
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  # Argumentos padrão passados ao Job (podem ser sobrescritos no start-job-run).
  default_arguments = {
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"    = var.dynamodb_table_nome
    "--DATASET_NAME" = "pedidos_desnormalizado"
    "--job-language" = "python"
    "--enable-metrics" = ""
    "--enable-continuous-cloudwatch-log" = "true"
  }

  tags = var.tags

  depends_on = [terraform_data.upload_script_glue]
}

# ---------------------------------------------------------------------------
# 5. Glue Data Catalog — database + tabelas (fato + 2 dimensões)
#    Registra o schema do gold para que o Athena possa consultá-lo via SQL.
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "gold" {
  name        = var.glue_database_nome
  description = "Banco de dados gold — modelo dimensional da prova Big Data UNIFAAT."
}

# Tabela: fato_pedidos (particionada por data_pedido)
resource "aws_glue_catalog_table" "fato_pedidos" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.gold.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"  = "parquet"
    "EXTERNAL"        = "TRUE"
    "parquet.compress" = "SNAPPY"
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

  # Coluna de partição — data_pedido é a chave de particionamento do fato.
  partition_keys {
    name = "data_pedido"
    type = "date"
  }

  depends_on = [terraform_data.bucket_gold]
}

# Tabela: dim_cliente (sem partição)
resource "aws_glue_catalog_table" "dim_cliente" {
  name          = "dim_cliente"
  database_name = aws_glue_catalog_database.gold.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"  = "parquet"
    "EXTERNAL"        = "TRUE"
    "parquet.compress" = "SNAPPY"
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

# Tabela: dim_produto (sem partição)
resource "aws_glue_catalog_table" "dim_produto" {
  name          = "dim_produto"
  database_name = aws_glue_catalog_database.gold.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"  = "parquet"
    "EXTERNAL"        = "TRUE"
    "parquet.compress" = "SNAPPY"
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

# ---------------------------------------------------------------------------
# 6. Athena Workgroup
#    Resultados gravados no próprio bucket gold (prefixo athena-results/).
# ---------------------------------------------------------------------------

resource "aws_athena_workgroup" "prova" {
  name        = var.athena_workgroup_nome
  description = "Workgroup Athena da prova Big Data UNIFAAT — RA 6325054."

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }

    # Força o uso do output location configurado (boa prática de custo/controle).
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
  }

  tags = var.tags

  depends_on = [terraform_data.bucket_gold]
}

# ---------------------------------------------------------------------------
# 7. DynamoDB — catálogo de metadados de execuções (Req 8.1–8.5)
#    PAY_PER_REQUEST: sem capacidade provisionada, sem custo residual.
#    Chave de partição: execution_id (string).
# ---------------------------------------------------------------------------

resource "aws_dynamodb_table" "execucoes" {
  name         = var.dynamodb_table_nome
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = var.tags
}
