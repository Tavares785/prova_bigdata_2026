# =====================================================================
# gold.tf - Camada GOLD (solução de referência)
# Premissas: provider (us-east-1) e variáveis regiao, labrole_arn,
# bucket_raw_nome, bucket_gold_nome e tags já estão declarados em
# raw.tf / variables.tf.
# =====================================================================

locals {
  script_key = "scripts/normaliza_pedidos.py"
  gold_uri   = "s3://${var.bucket_gold_nome}"
  tagset     = join(",", [for k, v in var.tags : "{Key=${k},Value=${v}}"])
}

# ---------------------------------------------------------------------
# 1 Bucket_Gold via AWS CLI (aws_s3_bucket é negado pelo SCP do Lab)
# ---------------------------------------------------------------------
resource "terraform_data" "bucket_gold" {
  # Em destroy só se pode usar self.*, por isso os valores ficam em input.
  input = {
    nome   = var.bucket_gold_nome
    regiao = var.regiao
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      set -e
      # us-east-1 NÃO aceita LocationConstraint
      aws s3api create-bucket --bucket ${var.bucket_gold_nome} --region ${var.regiao}
      aws s3api put-public-access-block --bucket ${var.bucket_gold_nome} \
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
      aws s3api put-bucket-encryption --bucket ${var.bucket_gold_nome} \
        --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
      aws s3api put-bucket-tagging --bucket ${var.bucket_gold_nome} \
        --tagging 'TagSet=[${local.tagset}]' || echo "aviso: tagging do bucket falhou"
    EOT
  }

  # Limpeza: o bucket contém objetos criados pelo Glue/Athena (fora do state),
  # então precisa ser esvaziado antes de remover.
  provisioner "local-exec" {
    when        = destroy
    interpreter = ["bash", "-c"]
    command     = "aws s3 rb s3://${self.input.nome} --force --region ${self.input.regiao}"
  }
}

# ---------------------------------------------------------------------
# 2 Script do Glue Job no S3 (prefixo scripts/, fora das tabelas)
# ---------------------------------------------------------------------
resource "aws_s3_object" "script" {
  bucket = var.bucket_gold_nome
  key    = local.script_key
  source = "${path.module}/../glue-job/normaliza_pedidos.py"
  etag   = filemd5("${path.module}/../glue-job/normaliza_pedidos.py")

  depends_on = [terraform_data.bucket_gold]
}

# ---------------------------------------------------------------------
# 3 DynamoDB - catálogo de execuções
# ---------------------------------------------------------------------
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

# ---------------------------------------------------------------------
# 4 Glue Job (LabRole por ARN, config econômica)
# ---------------------------------------------------------------------
resource "aws_glue_job" "normaliza" {
  name              = "normaliza-pedidos"
  role_arn          = var.labrole_arn
  glue_version      = "5.0" # Spark 3.5, alinhado ao teste local (PySpark 3.5.1)
  worker_type       = "G.1X"
  number_of_workers = 2 # mínimo do glueetl
  timeout           = 10 # minutos, evita job "preso" consumindo orçamento
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "${local.gold_uri}/${local.script_key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"  = "python"
    "--RAW_PATH"      = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"     = "${local.gold_uri}/"
    "--DDB_TABLE"     = aws_dynamodb_table.execucoes.name
    "--DATASET_NAME"  = "pedidos_desnormalizado"
  }

  tags       = var.tags
  depends_on = [aws_s3_object.script]
}

# ---------------------------------------------------------------------
# 5 Glue Data Catalog + tabelas
# ---------------------------------------------------------------------
resource "aws_glue_catalog_database" "gold" {
  name = "pedidos_gold"
}

locals {
  parquet_input  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
  parquet_output = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
  parquet_serde  = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
}

resource "aws_glue_catalog_table" "dim_cliente" {
  name          = "dim_cliente"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "parquet", EXTERNAL = "TRUE" }

  storage_descriptor {
    location      = "${local.gold_uri}/dim_cliente/"
    input_format  = local.parquet_input
    output_format = local.parquet_output
    ser_de_info { serialization_library = local.parquet_serde }

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
  parameters    = { classification = "parquet", EXTERNAL = "TRUE" }

  storage_descriptor {
    location      = "${local.gold_uri}/dim_produto/"
    input_format  = local.parquet_input
    output_format = local.parquet_output
    ser_de_info { serialization_library = local.parquet_serde }

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

# Fato particionada por data_pedido. Partition projection evita ter que
# rodar MSCK REPAIR / Crawler para as partições aparecerem no Athena.
resource "aws_glue_catalog_table" "fato_pedidos" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification                  = "parquet"
    EXTERNAL                        = "TRUE"
    "projection.enabled"            = "true"
    "projection.data_pedido.type"   = "date"
    "projection.data_pedido.format" = "yyyy-MM-dd"
    "projection.data_pedido.range"  = "2000-01-01,NOW" # ajuste se o dataset tiver datas futuras
    "storage.location.template"     = "${local.gold_uri}/fato_pedidos/data_pedido=$${data_pedido}/"
  }

  partition_keys {
    name = "data_pedido"
    type = "date"
  }

  storage_descriptor {
    location      = "${local.gold_uri}/fato_pedidos/"
    input_format  = local.parquet_input
    output_format = local.parquet_output
    ser_de_info { serialization_library = local.parquet_serde }

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
}

# ---------------------------------------------------------------------
# 6 Athena Workgroup
# ---------------------------------------------------------------------
resource "aws_athena_workgroup" "pedidos" {
  name          = "wg-pedidos"
  force_destroy = true # permite destruir mesmo com histórico de consultas

  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = 10485760 # 10 MB (mínimo), trava de custo

    result_configuration {
      output_location = "${local.gold_uri}/athena-results/"
    }
  }

  tags       = var.tags
  depends_on = [terraform_data.bucket_gold]
}
