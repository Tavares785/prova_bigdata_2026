# Camada gold da prova — AWS Academy Learner Lab, us-east-1.
variable "bucket_gold_nome" {
  type        = string
  description = "Bucket gold globalmente unico desta entrega."
  validation {
    condition     = can(regex("^prova-bigdata-gold-6325231-[a-z0-9-]+$", var.bucket_gold_nome)) && length(var.bucket_gold_nome) <= 63
    error_message = "Use prova-bigdata-gold-6325231-<sufixo>, ate 63 caracteres."
  }
}

variable "labrole_arn" {
  type        = string
  description = "ARN da LabRole preexistente no Learner Lab."
  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/(.*/)?LabRole$", var.labrole_arn))
    error_message = "Informe o ARN da LabRole preexistente."
  }
}

variable "tags" {
  type        = map(string)
  description = "Tags de identificacao e custo."
  default = {
    Projeto    = "prova-bigdata-6325231"
    Disciplina = "BigData"
    Ambiente   = "LearnerLab"
  }
  validation {
    condition = alltrue([
      for key in ["Projeto", "Disciplina", "Ambiente"] :
      can(regex("^[A-Za-z0-9_-]+$", var.tags[key]))
    ])
    error_message = "Informe Projeto, Disciplina e Ambiente com letras, numeros, _ ou -."
  }
}

locals {
  catalogo_nome = "prova_6325231"
  job_nome      = "normaliza-pedidos-6325231"
  ddb_nome      = "prova-6325231-execucoes"
  script_key    = "scripts/normaliza_pedidos.py"
}

# CLI evita a leitura de Object Lock bloqueada pela SCP do laboratorio.
resource "terraform_data" "bucket_gold" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }
  triggers_replace = [var.bucket_gold_nome, var.regiao]

  provisioner "local-exec" {
    command = <<-CMD
      set -eu
      if aws s3api head-bucket --bucket "${var.bucket_gold_nome}" --region "${var.regiao}" >/dev/null 2>&1; then
        echo "Bucket gold ja existe; verifique propriedade antes de aplicar." >&2
        exit 1
      fi
      aws s3api create-bucket --bucket "${var.bucket_gold_nome}" --region "${var.regiao}"
      aws s3api put-public-access-block --bucket "${var.bucket_gold_nome}" --region "${var.regiao}" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
      aws s3api put-bucket-tagging --bucket "${var.bucket_gold_nome}" --region "${var.regiao}" --tagging '${jsonencode({ TagSet = [for key, value in var.tags : { Key = key, Value = value }] })}'
    CMD
  }

  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force --region ${self.input.regiao}"
  }
}

resource "terraform_data" "script_glue" {
  input = {
    bucket = var.bucket_gold_nome
    key    = local.script_key
  }
  triggers_replace = [filesha256("${path.module}/../glue-job/normaliza_pedidos.py"), var.bucket_gold_nome]
  depends_on       = [terraform_data.bucket_gold]

  provisioner "local-exec" {
    command = "aws s3 cp \"${path.module}/../glue-job/normaliza_pedidos.py\" \"s3://${var.bucket_gold_nome}/${local.script_key}\" --region \"${var.regiao}\""
  }
}

resource "aws_dynamodb_table" "execucoes" {
  name         = local.ddb_nome
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"
  attribute {
    name = "execution_id"
    type = "S"
  }
  tags = var.tags
}

resource "aws_glue_job" "normaliza" {
  name              = local.job_nome
  role_arn          = var.labrole_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 15
  max_retries       = 0
  execution_property {
    max_concurrent_runs = 1
  }
  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/${local.script_key}"
    python_version  = "3"
  }
  default_arguments = {
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"    = aws_dynamodb_table.execucoes.name
    "--DATASET_NAME" = "pedidos_desnormalizado"
    "--job-language" = "python"
  }
  tags       = var.tags
  depends_on = [terraform_data.bucket_raw, terraform_data.script_glue]
}

resource "aws_glue_catalog_database" "gold" {
  name = local.catalogo_nome
  tags = var.tags
}

resource "aws_glue_catalog_table" "dim_cliente" {
  name          = "dim_cliente"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { EXTERNAL = "TRUE", classification = "parquet" }
  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/dim_cliente/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info { serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" }
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
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { EXTERNAL = "TRUE", classification = "parquet" }
  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/dim_produto/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info { serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" }
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

resource "aws_glue_catalog_table" "fato_pedidos" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    EXTERNAL                               = "TRUE"
    classification                         = "parquet"
    "projection.enabled"                   = "true"
    "projection.data_pedido.type"          = "date"
    "projection.data_pedido.format"        = "yyyy-MM-dd"
    "projection.data_pedido.range"         = "2020-01-01,NOW"
    "projection.data_pedido.interval"      = "1"
    "projection.data_pedido.interval.unit" = "DAYS"
    "storage.location.template"            = "s3://${var.bucket_gold_nome}/fato_pedidos/data_pedido=$${data_pedido}/"
  }
  partition_keys {
    name = "data_pedido"
    type = "string"
  }
  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/fato_pedidos/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info { serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" }
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
  depends_on = [terraform_data.bucket_gold]
}

resource "aws_athena_workgroup" "prova" {
  name          = "prova-6325231"
  force_destroy = true
  configuration {
    enforce_workgroup_configuration = true
    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }
  tags       = var.tags
  depends_on = [terraform_data.bucket_gold]
}

output "job_nome" { value = aws_glue_job.normaliza.name }
output "catalogo_nome" { value = aws_glue_catalog_database.gold.name }
output "workgroup_nome" { value = aws_athena_workgroup.prova.name }
output "ddb_nome" { value = aws_dynamodb_table.execucoes.name }

# O raw.tf é fornecido e preservado; esta etapa só aplica as tags exigidas.
resource "terraform_data" "tags_raw" {
  input = {
    bucket = var.bucket_raw_nome
    tags   = var.tags
  }
  triggers_replace = [var.bucket_raw_nome, var.tags]
  depends_on       = [terraform_data.bucket_raw]

  provisioner "local-exec" {
    command = "aws s3api put-bucket-tagging --bucket \"${var.bucket_raw_nome}\" --region \"${var.regiao}\" --tagging '${jsonencode({ TagSet = [for key, value in var.tags : { Key = key, Value = value }] })}'"
  }
}
