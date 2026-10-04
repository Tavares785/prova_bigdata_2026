# gold.tf — CAMADA GOLD
# =============================================================================
# Usa estas variáveis (devem existir no variables.tf):
#   var.regiao, var.bucket_raw_nome   -> já usadas pelo raw.tf
#   var.labrole_arn                   -> ARN da LabRole (arn:aws:iam::<CONTA>:role/LabRole)
#   var.bucket_gold_nome              -> nome único global do bucket gold
#   var.tags                          -> map(string) com Projeto, Disciplina, Ambiente
# Não declara variáveis nem provider aqui (o provider está no raw.tf).
# =============================================================================

locals {
  glue_job_nome = "normaliza-pedidos"
  glue_db_nome  = "prova_bigdata_gold"
  ddb_tabela    = "execucoes"
  athena_wg     = "prova-bigdata"

  gold_s3       = "s3://${var.bucket_gold_nome}"
  script_local  = "${path.module}/../glue-job/normaliza_pedidos.py"
  script_s3_key = "scripts/normaliza_pedidos.py"

  # Tags no formato JSON que o `aws s3api put-bucket-tagging` espera.
  tags_bucket_json = jsonencode({
    TagSet = [for k, v in var.tags : { Key = k, Value = v }]
  })
}

# -----------------------------------------------------------------------------
# Bucket_Gold — criado via AWS CLI (NÃO usar aws_s3_bucket: a SCP do Learner Lab
# nega s3:GetBucketObjectLockConfiguration). Privado, com os 4 bloqueios.
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
      aws s3api head-bucket --bucket "${var.bucket_gold_nome}" 2>/dev/null || \
        aws s3api create-bucket --bucket "${var.bucket_gold_nome}" --region "${var.regiao}"
      aws s3api put-public-access-block --bucket "${var.bucket_gold_nome}" \
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
      aws s3api put-bucket-tagging --bucket "${var.bucket_gold_nome}" \
        --tagging '${local.tags_bucket_json}' || echo "aviso: nao foi possivel aplicar tags no bucket"
    CMD
  }

  # DESTROY: esvazia e remove o Bucket_Gold.
  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}

# Sobe o script PySpark para o gold (prefixo scripts/). Reenvia se o arquivo mudar.
resource "terraform_data" "glue_script" {
  triggers_replace = [filemd5(local.script_local), var.bucket_gold_nome]

  provisioner "local-exec" {
    command = "aws s3 cp \"${local.script_local}\" \"${local.gold_s3}/${local.script_s3_key}\""
  }

  depends_on = [terraform_data.bucket_gold]
}

# -----------------------------------------------------------------------------
# Glue Job (PySpark) — usa a LabRole por ARN, sem criar IAM.
# -----------------------------------------------------------------------------
resource "aws_glue_job" "normaliza" {
  name              = local.glue_job_nome
  role_arn          = var.labrole_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2 # mínimo para G.1X
  max_retries       = 0
  timeout           = 10 # minutos: evita job preso gastando orçamento

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${local.gold_s3}/${local.script_s3_key}"
  }

  # Valores padrão: o start-job-run funciona mesmo sem --arguments.
  default_arguments = {
    "--job-language" = "python"
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "${local.gold_s3}/"
    "--DDB_TABLE"    = local.ddb_tabela
    "--DATASET_NAME" = "pedidos_desnormalizado"
  }

  tags = var.tags

  depends_on = [terraform_data.glue_script]
}

# -----------------------------------------------------------------------------
# DynamoDB — catálogo de metadados das execuções.
# -----------------------------------------------------------------------------
resource "aws_dynamodb_table" "execucoes" {
  name         = local.ddb_tabela
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = var.tags
}

# -----------------------------------------------------------------------------
# Glue Data Catalog — database + 3 tabelas externas Parquet.
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_database" "gold" {
  name = local.glue_db_nome
  tags = var.tags
}

resource "aws_glue_catalog_table" "dim_cliente" {
  name          = "dim_cliente"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "parquet", EXTERNAL = "TRUE" }

  storage_descriptor {
    location      = "${local.gold_s3}/dim_cliente/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
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
  parameters    = { classification = "parquet", EXTERNAL = "TRUE" }

  storage_descriptor {
    location      = "${local.gold_s3}/dim_produto/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
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

# fato_pedidos particionada por data_pedido. Usa partition projection do Athena:
# as partições data_pedido=YYYY-MM-DD são "descobertas" pelo padrão do caminho,
# então não precisa rodar MSCK REPAIR TABLE depois do Job.
resource "aws_glue_catalog_table" "fato_pedidos" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification                  = "parquet"
    EXTERNAL                        = "TRUE"
    "projection.enabled"            = "true"
    "projection.data_pedido.type"   = "date"
    "projection.data_pedido.range"  = "2025-01-01,NOW"
    "projection.data_pedido.format" = "yyyy-MM-dd"
    "projection.data_pedido.interval"      = "1"
    "projection.data_pedido.interval.unit" = "DAYS"
    "storage.location.template"     = "${local.gold_s3}/fato_pedidos/data_pedido=$${data_pedido}/"
  }

  partition_keys {
    name = "data_pedido"
    type = "string"
  }

  storage_descriptor {
    location      = "${local.gold_s3}/fato_pedidos/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
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

# -----------------------------------------------------------------------------
# Athena Workgroup — resultados das consultas em s3://<gold>/athena-results/.
# -----------------------------------------------------------------------------
resource "aws_athena_workgroup" "prova" {
  name          = local.athena_wg
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false

    result_configuration {
      output_location = "${local.gold_s3}/athena-results/"
    }
  }

  tags = var.tags

  depends_on = [terraform_data.bucket_gold]
}

# -----------------------------------------------------------------------------
# Outputs úteis para a execução e para os prints de evidência.
# -----------------------------------------------------------------------------
output "bucket_gold" {
  value = var.bucket_gold_nome
}

output "athena_workgroup" {
  value = aws_athena_workgroup.prova.name
}

output "glue_database" {
  value = aws_glue_catalog_database.gold.name
}

output "comando_start_job" {
  value = "aws glue start-job-run --job-name ${aws_glue_job.normaliza.name} --region ${var.regiao}"
}