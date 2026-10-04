# =============================================================================
# gold.tf — Camada GOLD (RA 6325127)
#   Bucket_Gold + script do Glue + Glue Job + Glue Data Catalog + Athena + DynamoDB
#   Substitua cada ______
# =============================================================================

# -----------------------------------------------------------------------------
# 0) VARIÁVEIS da camada gold
# -----------------------------------------------------------------------------
variable "bucket_gold_nome" {
  description = "Nome global único do bucket S3 gold."
  type        = string
}

variable "labrole_arn" {
  description = "ARN da LabRole do Learner Lab (NÃO criamos roles/policies)."
  type        = string
}

variable "tags" {
  description = "Tags de custo padronizadas."
  type        = map(string)
  default = {
    Projeto    = "prova-bigdata"
    Disciplina = "BigData-UNIFAAT"
    Ambiente   = "prova"
  }
}

locals {
  glue_job_nome = "normaliza-pedidos"
  ddb_tabela    = "execucoes"
  script_local  = "${path.module}/../glue-job/normaliza_pedidos.py"
  script_s3     = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
}

# -----------------------------------------------------------------------------
# 1) BUCKET GOLD — mesmo padrão do raw.tf (AWS CLI, NÃO usar aws_s3_bucket)
# -----------------------------------------------------------------------------
resource "terraform_data" "bucket_gold" {
  input = {
    bucket = var.bucket_gold_nome
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

  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}

# -----------------------------------------------------------------------------
# 2) UPLOAD DO SCRIPT do Glue para o bucket gold (re-sobe se o .py mudar)
# -----------------------------------------------------------------------------
resource "terraform_data" "script_glue" {
  triggers_replace = [filemd5(local.script_local)]
  depends_on       = [terraform_data.bucket_gold]

  provisioner "local-exec" {
    command = "aws s3 cp \"${local.script_local}\" \"${local.script_s3}\""
  }
}

# -----------------------------------------------------------------------------
# 3) GLUE JOB (PySpark) usando a LabRole por ARN
# -----------------------------------------------------------------------------
resource "aws_glue_job" "normaliza" {
  name              = local.glue_job_nome
  role_arn          = var.labrole_arn            # LabRole (qual variável tem o ARN?)
  glue_version      = "4.0"
  worker_type       = "G.1X"                 # menor worker (econômico)
  number_of_workers = 2                      # mínimo (econômico)
  timeout           = 10                     # minutos — evita gastar orçamento

  command {
    name            = "glueetl"               # tipo de job Spark (README seção 7)
    script_location = local.script_s3
    python_version  = "3"
  }

  # Argumentos padrão: assim o start-job-run não precisa de --arguments
  default_arguments = {
    "--job-language" = "python"
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"    = local.ddb_tabela
    "--DATASET_NAME" = "pedidos_desnormalizado"
  }

  tags       = var.tags
  depends_on = [terraform_data.script_glue]
}

# -----------------------------------------------------------------------------
# 4) DYNAMODB — catálogo de metadados das execuções
# -----------------------------------------------------------------------------
resource "aws_dynamodb_table" "execucoes" {
  name         = local.ddb_tabela
  billing_mode = "PAY_PER_REQUEST"                    # sem capacidade provisionada (README seção 9)
  hash_key     = "execution_id"                    # chave de partição (README seção 9)

  attribute {
    name = "execution_id"                          # mesma coluna do hash_key
    type = "S"                               # S = String
  }

  tags = var.tags
}

# -----------------------------------------------------------------------------
# 5) GLUE DATA CATALOG — banco + 3 tabelas Parquet apontando pro gold
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_database" "gold" {
  name = "prova_gold"
}

resource "aws_glue_catalog_table" "fato_pedidos" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "parquet", EXTERNAL = "TRUE" }

  # coluna de partição (vira pasta data_pedido=YYYY-MM-DD/)
  partition_keys {
    name = "data_pedido"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/fato_pedidos/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    # a coluna de partição NÃO entra aqui — ela já está em partition_keys
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

resource "aws_glue_catalog_table" "dim_cliente" {
  name          = "dim_cliente"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "parquet", EXTERNAL = "TRUE" }

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
}

resource "aws_glue_catalog_table" "dim_produto" {
  name          = "dim_produto"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "parquet", EXTERNAL = "TRUE" }

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
}

# -----------------------------------------------------------------------------
# 6) ATHENA WORKGROUP — resultados das consultas vão pro bucket gold
# -----------------------------------------------------------------------------
resource "aws_athena_workgroup" "prova" {
  name          = "prova-bigdata"
  force_destroy = true                       # deixa o destroy apagar o histórico

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"   # pasta dos resultados
    }
  }

  tags       = var.tags
  depends_on = [terraform_data.bucket_gold]
}

# -----------------------------------------------------------------------------
# 7) OUTPUTS — facilitam os comandos da prova
# -----------------------------------------------------------------------------
output "glue_job_nome" {
  value = aws_glue_job.normaliza.name
}

output "bucket_gold" {
  value = var.bucket_gold_nome
}

output "dynamodb_tabela" {
  value = aws_dynamodb_table.execucoes.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.prova.name
}
