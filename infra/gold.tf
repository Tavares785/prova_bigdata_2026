# gold.tf — CAMADA GOLD  ✍️ ALUNO (RA 6325033)
# =============================================================================
# Provisiona toda a camada gold:
#   - Bucket_Gold (criado via AWS CLI — mesmo padrão do raw.tf, evita SCP block)
#   - Upload do script PySpark para o Bucket_Gold
#   - Referência à LabRole por ARN (sem criar roles/policies próprias)
#   - Glue Job (glueetl / PySpark)
#   - Glue Data Catalog (database + tabelas fato e dimensões)
#   - Athena Workgroup com output location no Bucket_Gold
#   - DynamoDB — tabela de catálogo de execuções (PAY_PER_REQUEST)
#
# ⚠️  NÃO use aws_s3_bucket: a SCP do Learner Lab nega
#     s3:GetBucketObjectLockConfiguration, que o recurso sempre lê -> AccessDenied.
#     Buckets são criados privados via AWS CLI (terraform_data + local-exec).
# Requirements: 4.1–4.6, 5.1–5.3, 7.1–7.4, 8.1–8.5, 10.3, 11.2, 11.3.
# =============================================================================

# -----------------------------------------------------------------------------
# 1. Bucket_Gold — criado via AWS CLI (privado, public-access-block habilitado).
# -----------------------------------------------------------------------------
resource "terraform_data" "bucket_gold" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }

  triggers_replace = [var.bucket_gold_nome, var.regiao]

  provisioner "local-exec" {
    interpreter = ["powershell", "-Command"]
    command     = <<-PS
      aws s3api create-bucket --bucket "${var.bucket_gold_nome}" --region "${var.regiao}" 2>$null; `
      aws s3api put-public-access-block --bucket "${var.bucket_gold_nome}" `
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
    PS
  }

  # DESTROY: esvazia e remove o Bucket_Gold no terraform destroy.
  provisioner "local-exec" {
    when        = destroy
    interpreter = ["powershell", "-Command"]
    command     = "aws s3 rb s3://${self.input.bucket} --force; exit 0"
  }
}

# -----------------------------------------------------------------------------
# 2. Upload do script PySpark para o Bucket_Gold (scripts/).
#    O Glue Job lê o script diretamente do S3.
# -----------------------------------------------------------------------------
resource "terraform_data" "upload_script_glue" {
  input = {
    bucket = var.bucket_gold_nome
  }

  triggers_replace = [
    var.bucket_gold_nome,
    filemd5("${path.module}/../glue-job/normaliza_pedidos.py"),
  ]

  depends_on = [terraform_data.bucket_gold]

  provisioner "local-exec" {
    interpreter = ["powershell", "-Command"]
    command     = <<-PS
      aws s3 cp "${path.module}/../glue-job/normaliza_pedidos.py" `
        "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    PS
  }
}

# -----------------------------------------------------------------------------
# 3. Referência à LabRole por ARN — sem criar roles/policies próprias.
#    (Req 11.3: IAM restrito no Learner Lab)
# -----------------------------------------------------------------------------
data "aws_iam_role" "labrole" {
  name = element(split("/", var.labrole_arn), length(split("/", var.labrole_arn)) - 1)
}

# -----------------------------------------------------------------------------
# 4. Glue Data Catalog — database que agrupa as tabelas do gold.
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_database" "gold" {
  name        = var.glue_database_nome
  description = "Banco de dados do gold — esquema estrela normalizado (fato + dimensões)."

  tags = var.tags
}

# -----------------------------------------------------------------------------
# 5. Glue Data Catalog — tabela fato_pedidos (particionada por data_pedido).
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_table" "fato_pedidos" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.gold.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"       = "parquet"
    "parquet.compression"  = "SNAPPY"
    "EXTERNAL"             = "TRUE"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/fato_pedidos/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet-serde"
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

  depends_on = [terraform_data.bucket_gold]
}

# -----------------------------------------------------------------------------
# 6. Glue Data Catalog — tabela dim_cliente (sem partição).
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_table" "dim_cliente" {
  name          = "dim_cliente"
  database_name = aws_glue_catalog_database.gold.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"       = "parquet"
    "parquet.compression"  = "SNAPPY"
    "EXTERNAL"             = "TRUE"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/dim_cliente/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet-serde"
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

# -----------------------------------------------------------------------------
# 7. Glue Data Catalog — tabela dim_produto (sem partição).
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_table" "dim_produto" {
  name          = "dim_produto"
  database_name = aws_glue_catalog_database.gold.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"       = "parquet"
    "parquet.compression"  = "SNAPPY"
    "EXTERNAL"             = "TRUE"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/dim_produto/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet-serde"
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
# 8. Glue Job — glueetl (PySpark), usa a LabRole por ARN.
#    Workers econômicos (G.1X, 2 workers) para minimizar custo no Learner Lab.
# -----------------------------------------------------------------------------
resource "aws_glue_job" "normaliza_pedidos" {
  name     = var.glue_job_nome
  role_arn = data.aws_iam_role.labrole.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  default_arguments = {
    "--RAW_PATH"                     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"                    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"                    = var.dynamodb_tabela_nome
    "--DATASET_NAME"                 = "pedidos_desnormalizado"
    "--job-language"                 = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"               = "true"
    "--TempDir"                      = "s3://${var.bucket_gold_nome}/tmp/"
  }

  timeout            = 10  # minutos — Job simples, evita consumo excessivo
  max_retries        = 0

  depends_on = [
    terraform_data.upload_script_glue,
    aws_glue_catalog_database.gold,
  ]

  tags = var.tags
}

# -----------------------------------------------------------------------------
# 9. Athena Workgroup — output location no Bucket_Gold (pasta query-results/).
# -----------------------------------------------------------------------------
resource "aws_athena_workgroup" "prova" {
  name        = var.athena_workgroup_nome
  description = "Workgroup Athena para consultas analíticas sobre o gold (prova Big Data)."

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false

    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/query-results/"
    }
  }

  force_destroy = true

  depends_on = [terraform_data.bucket_gold]

  tags = var.tags
}

# -----------------------------------------------------------------------------
# 10. DynamoDB — tabela de catálogo de execuções do Glue Job.
#     Chave de partição: execution_id (string). PAY_PER_REQUEST — sem custo residual.
# -----------------------------------------------------------------------------
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
