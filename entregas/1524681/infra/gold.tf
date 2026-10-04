# gold.tf — camada gold: Bucket_Gold (privado, via AWS CLI), script PySpark + Glue Job,
# Glue Data Catalog (database + 3 tabelas), Athena Workgroup e DynamoDB de metadados.

locals {
  # Nome do database no Glue Data Catalog: só minúsculas/números/underscore —
  # o nome do bucket (com hífens) é convertido para caber na regra.
  catalog_db = replace(var.bucket_gold_nome, "-", "_")
  gold_base  = "s3://${var.bucket_gold_nome}"

  # TagSet no formato aceito pela AWS CLI: como o Bucket_Gold é criado pela CLI,
  # as tags de custo são aplicadas com put-bucket-tagging logo após a criação.
  tags_cli = "TagSet=[${join(",", [for k, v in var.tags : "{Key=${k},Value=${v}}"])}]"
}

# --- Bucket_Gold: criado via CLI, PRIVADO (4 bloqueios) + criptografia SSE-S3.
#     A criação é via AWS CLI (terraform_data + local-exec) porque a SCP da
#     organização do Learner Lab NEGA s3:GetBucketObjectLockConfiguration, que o
#     recurso aws_s3_bucket sempre lê -> AccessDenied.
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
      aws s3api put-bucket-encryption --bucket "${var.bucket_gold_nome}" \
        --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
      aws s3api put-bucket-tagging --bucket "${var.bucket_gold_nome}" \
        --tagging '${local.tags_cli}'
    CMD
  }

  # DESTROY: esvazia e remove o Bucket_Gold no terraform destroy (o script do
  # Glue, os Parquet do gold e os resultados do Athena morrem junto).
  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}

# --- Sobe o script PySpark para o S3 (o Glue Job executa a partir daí).
#     Reexecuta somente quando o conteúdo do script muda (filemd5).
resource "terraform_data" "glue_script" {
  triggers_replace = [filemd5("${path.module}/../glue-job/normaliza_pedidos.py")]

  provisioner "local-exec" {
    command = <<-CMD
      set -e
      aws s3 cp "${path.module}/../glue-job/normaliza_pedidos.py" \
        "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    CMD
  }

  depends_on = [terraform_data.bucket_gold]
}

# --- Glue Data Catalog: database das tabelas do gold.
resource "aws_glue_catalog_database" "gold" {
  name = local.catalog_db
  tags = var.tags
}

# --- Tabelas do gold registradas no catálogo (sem Crawler; partições do fato
#     são registradas com MSCK REPAIR TABLE após a execução do job).
resource "aws_glue_catalog_table" "fato_pedidos" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "parquet" }

  storage_descriptor {
    location      = "${local.gold_base}/fato_pedidos/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "fato_pedidos-parquet-serde"
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

  # data_pedido sai do arquivo Parquet e vira o diretório de partição
  # (data_pedido=YYYY-MM-DD/) — por isso está em partition_keys, não em columns.
  partition_keys {
    name = "data_pedido"
    type = "string"
  }
}

resource "aws_glue_catalog_table" "dim_cliente" {
  name          = "dim_cliente"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "parquet" }

  storage_descriptor {
    location      = "${local.gold_base}/dim_cliente/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "dim_cliente-parquet-serde"
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
  parameters    = { classification = "parquet" }

  storage_descriptor {
    location      = "${local.gold_base}/dim_produto/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "dim_produto-parquet-serde"
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

# --- Glue Job: executa o script PySpark com a LabRole por ARN (nenhuma role ou
#     policy IAM própria é criada). Glue 5.0 + G.1X × 2 workers (Glue 4/5 não
#     aceita max_capacity); timeout = 10 (em MINUTOS, como na API do Glue) limita
#     o custo de uma execução travada. Se o Lab não oferecer Glue 5.0, use
#     glue_version = "4.0" (o script é compatível com ambos).
resource "aws_glue_job" "normaliza_pedidos" {
  name         = "normaliza-pedidos"
  role_arn     = var.labrole_arn
  glue_version = "5.0"

  command {
    name            = "glueetl"
    script_location = "${local.gold_base}/scripts/normaliza_pedidos.py"
  }

  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 10
  max_retries       = 0

  default_arguments = {
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/pedidos_desnormalizado.csv"
    "--GOLD_PATH"    = local.gold_base
    "--DDB_TABLE"    = aws_dynamodb_table.metadados.name
    "--DATASET_NAME" = "pedidos_desnormalizado"
  }

  tags = var.tags

  depends_on = [terraform_data.glue_script]
}

# --- Athena: workgroup com resultados gravados no Bucket_Gold.
resource "aws_athena_workgroup" "prova" {
  name          = "workgroup-prova-bigdata"
  description   = "Workgroup do Athena para as consultas da camada gold."
  force_destroy = true

  configuration {
    result_configuration {
      output_location = "${local.gold_base}/athena-results/"
    }
  }

  tags = var.tags
}

# --- DynamoDB: metadados das execuções (execution_id é a chave de partição).
resource "aws_dynamodb_table" "metadados" {
  name         = "metadados_execucoes"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = var.tags
}