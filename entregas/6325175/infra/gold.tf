# gold.tf — CAMADA GOLD
# =============================================================================
# Provisiona toda a infraestrutura da camada gold e de suporte:
#   1. Bucket_Gold (privado, 4 bloqueios de acesso público) via AWS CLI
#   2. Upload do script PySpark para o Bucket_Gold
#   3. Glue Job normaliza-pedidos (Glue 4.0, G.1X, 2 workers)
#   4. Glue Data Catalog — database prova_bigdata
#   5. Glue Data Catalog — tabela fato_pedidos (Parquet, Partition Projection)
#   6. Glue Data Catalog — tabela dim_cliente (Parquet, sem partição)
#   7. Glue Data Catalog — tabela dim_produto (Parquet, sem partição)
#   8. Athena Workgroup com output no Bucket_Gold
#   9. DynamoDB execucoes (PAY_PER_REQUEST, hash_key execution_id)
#
# ⚠️  Learner Lab: buckets criados via AWS CLI (terraform_data + local-exec).
#      O recurso aws_s3_bucket NÃO é usado porque a SCP da organização Academy
#      nega s3:GetBucketObjectLockConfiguration, causando AccessDenied no refresh.
#
# Requirements: Req 10.2, 10.3, 11.2, 11.5, 13.1, 13.4, 13.5, 13.6
# =============================================================================

# Data source para obter o account_id da conta AWS ativa.
# Necessário para o catalog_id do Glue Data Catalog Database.
data "aws_caller_identity" "current" {}

# =============================================================================
# 1. Bucket_Gold — criado via AWS CLI (padrão idêntico ao raw.tf)
# Req 10.2, 11.2, 13.1.2, 13.6.2
# =============================================================================
resource "terraform_data" "bucket_gold" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }

  triggers_replace = [var.bucket_gold_nome, var.regiao]

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]

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

# =============================================================================
# 2. Upload do script PySpark para o Bucket_Gold
# Req 13.1.3
# Nota: se a SCP bloquear s3:PutObject do provider Terraform, substituir por
#   terraform_data + local-exec com "aws s3 cp" (mesmo padrão de raw.tf).
# =============================================================================
resource "aws_s3_object" "script_normalizacao" {
  bucket = var.bucket_gold_nome
  key    = "scripts/normaliza_pedidos.py"
  source = "${path.module}/../glue-job/normaliza_pedidos.py"
  etag   = filemd5("${path.module}/../glue-job/normaliza_pedidos.py")

  depends_on = [terraform_data.bucket_gold]
}

# =============================================================================
# 3. Glue Job normaliza-pedidos
# Glue 4.0, Python 3.10, WorkerType G.1X, 2 workers, role via var.labrole_arn.
# Req 10.3, 11.5, 13.1.3, 13.6.4
# =============================================================================
resource "aws_glue_job" "normaliza_pedidos" {
  name              = "normaliza-pedidos"
  role_arn          = var.labrole_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language" = "python"
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"    = "execucoes"
    "--DATASET_NAME" = "pedidos_desnormalizado"
  }

  tags = var.tags

  depends_on = [aws_s3_object.script_normalizacao]
}

# =============================================================================
# 4. Glue Data Catalog — Database prova_bigdata
# Req 13.1.4
# Nota: aws_glue_catalog_database NÃO aceita o argumento "tags" no provider
#   hashicorp/aws 5.31.0 — a propriedade não existe nessa versão do recurso.
#   Tags são aplicadas nos demais recursos (Glue Job, Athena, DynamoDB).
# =============================================================================
resource "aws_glue_catalog_database" "prova" {
  name       = "prova_bigdata"
  catalog_id = data.aws_caller_identity.current.account_id
}

# =============================================================================
# 5. Glue Data Catalog — Tabela fato_pedidos (Parquet, Partition Projection)
# A Partition Projection elimina a necessidade de Crawler ou MSCK REPAIR TABLE:
#   o Athena resolve partições em tempo de consulta a partir do parâmetro de range.
# Req 4.3, 13.1.4, 13.4
# =============================================================================
resource "aws_glue_catalog_table" "fato_pedidos" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.prova.name
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/fato_pedidos/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # Colunas do fato (data_pedido é partition_key — não entra aqui).
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

  # Chave de partição.
  partition_keys {
    name = "data_pedido"
    type = "date"
  }

  # Partition Projection — Athena resolve partições sem Crawler.
  # $${data_pedido} é a forma de escapar $ no Terraform para que
  # o valor literal "${data_pedido}" chegue ao parâmetro do Athena.
  parameters = {
    "classification"                       = "parquet"
    "projection.enabled"                   = "true"
    "projection.data_pedido.type"          = "date"
    "projection.data_pedido.range"         = "2020-01-01,2030-12-31"
    "projection.data_pedido.format"        = "yyyy-MM-dd"
    "projection.data_pedido.interval"      = "1"
    "projection.data_pedido.interval.unit" = "DAYS"
    "storage.location.template"            = "s3://${var.bucket_gold_nome}/fato_pedidos/data_pedido=$${data_pedido}"
  }
}

# =============================================================================
# 6. Glue Data Catalog — Tabela dim_cliente (Parquet, sem partição)
# Req 4.1, 13.1.4, 13.4
# =============================================================================
resource "aws_glue_catalog_table" "dim_cliente" {
  name          = "dim_cliente"
  database_name = aws_glue_catalog_database.prova.name
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/dim_cliente/"
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

  parameters = {
    "classification" = "parquet"
  }
}

# =============================================================================
# 7. Glue Data Catalog — Tabela dim_produto (Parquet, sem partição)
# Req 4.2, 13.1.4, 13.4
# =============================================================================
resource "aws_glue_catalog_table" "dim_produto" {
  name          = "dim_produto"
  database_name = aws_glue_catalog_database.prova.name
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/dim_produto/"
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

  parameters = {
    "classification" = "parquet"
  }
}

# =============================================================================
# 8. Athena Workgroup
# Resultados gravados em s3://<gold>/athena-results/
# Req 13.1.5, 13.4
# =============================================================================
resource "aws_athena_workgroup" "prova" {
  name = "prova-workgroup"

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  tags = var.tags
}

# =============================================================================
# 9. DynamoDB — tabela execucoes (metadados de execução do Glue Job)
# billing_mode PAY_PER_REQUEST, hash_key execution_id (tipo S).
# Req 6, 13.1.6, 13.5, 13.6.7
# =============================================================================
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
