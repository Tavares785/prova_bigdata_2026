# Daniel de Oliveira Tavares Junior — RA 6325032.
# Buckets via AWS CLI, sem aws_s3_bucket (restrição do Learner Lab).
locals {
  gold_prefixo = "prova-bigdata-6325032"
  gold_tags_s3 = jsonencode({ TagSet = [for chave, valor in var.tags : { Key = chave, Value = valor }] })
  gold_tabelas = {
    dim_cliente = [
      { name = "cliente_id", type = "string" },
      { name = "cliente_nome", type = "string" },
      { name = "cliente_uf", type = "string" },
    ]
    dim_produto = [
      { name = "produto_id", type = "string" },
      { name = "produto_nome", type = "string" },
      { name = "categoria", type = "string" },
    ]
    fato_pedidos = [
      { name = "pedido_id", type = "string" },
      { name = "cliente_id", type = "string" },
      { name = "produto_id", type = "string" },
      { name = "preco_unitario", type = "double" },
      { name = "quantidade", type = "int" },
      { name = "valor_total", type = "double" },
    ]
  }
}

resource "terraform_data" "bucket_gold" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }
  triggers_replace = [var.bucket_gold_nome, var.regiao]

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    environment = {
      GOLD_BUCKET        = self.input.bucket
      AWS_DEFAULT_REGION = self.input.regiao
      AWS_PAGER          = ""
      OWNER_TAG          = jsonencode({ TagSet = [{ Key = "TerraformOwner", Value = self.id }] })
    }
    # Não adota bucket existente: o destroy só deve apagar o bucket deste projeto.
    command = <<-CMD
      set -euo pipefail
      aws --version 2>&1 | grep -q 'aws-cli/2\.'
      if aws s3api head-bucket --bucket "$GOLD_BUCKET" >/dev/null 2>&1; then
        echo "Bucket gold já existe. Escolha um nome novo antes de aplicar." >&2
        exit 1
      fi
      aws s3api create-bucket --bucket "$GOLD_BUCKET" --region "$AWS_DEFAULT_REGION"
      aws s3api put-bucket-tagging --bucket "$GOLD_BUCKET" --tagging "$OWNER_TAG"
      aws s3api put-public-access-block --bucket "$GOLD_BUCKET" \
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
      aws s3api put-bucket-encryption --bucket "$GOLD_BUCKET" \
        --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
    CMD
  }

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["/bin/bash", "-c"]
    environment = {
      GOLD_BUCKET        = self.input.bucket
      AWS_DEFAULT_REGION = self.input.regiao
      AWS_PAGER          = ""
      OWNER_ID           = self.id
    }
    command = <<-CMD
      set -euo pipefail
      owner=$(aws s3api get-bucket-tagging --bucket "$GOLD_BUCKET" --query 'TagSet[?Key==`TerraformOwner`].Value | [0]' --output text)
      if [ "$owner" != "$OWNER_ID" ]; then
        echo "O bucket não tem a identificação deste recurso Terraform; remoção interrompida." >&2
        exit 1
      fi
      aws s3 rb "s3://$GOLD_BUCKET" --force --region "$AWS_DEFAULT_REGION"
    CMD
  }
}

# Tags atualizadas sem recriar buckets ou alterar raw.tf.
resource "terraform_data" "tags_buckets" {
  triggers_replace = [terraform_data.bucket_raw.id, terraform_data.bucket_gold.id, local.gold_tags_s3]
  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    environment = {
      RAW_BUCKET         = var.bucket_raw_nome
      GOLD_BUCKET        = var.bucket_gold_nome
      TAGS_JSON          = local.gold_tags_s3
      GOLD_TAGS_JSON     = jsonencode({ TagSet = concat([for chave, valor in var.tags : { Key = chave, Value = valor }], [{ Key = "TerraformOwner", Value = terraform_data.bucket_gold.id }]) })
      AWS_DEFAULT_REGION = var.regiao
      AWS_PAGER          = ""
    }
    command = <<-CMD
      set -euo pipefail
      aws s3api put-bucket-tagging --bucket "$RAW_BUCKET" --tagging "$TAGS_JSON"
      aws s3api put-bucket-tagging --bucket "$GOLD_BUCKET" --tagging "$GOLD_TAGS_JSON"
    CMD
  }
}

resource "aws_s3_object" "script_normalizacao" {
  bucket                 = terraform_data.bucket_gold.output.bucket
  key                    = "scripts/normaliza_pedidos.py"
  source                 = "${path.module}/../glue-job/normaliza_pedidos.py"
  source_hash            = filemd5("${path.module}/../glue-job/normaliza_pedidos.py")
  content_type           = "text/x-python"
  server_side_encryption = "AES256"
  tags                   = var.tags
}

resource "aws_dynamodb_table" "execucoes" {
  name         = "${local.gold_prefixo}-execucoes"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"
  attribute {
    name = "execution_id"
    type = "S"
  }
  tags = var.tags
}

resource "aws_glue_job" "normaliza_pedidos" {
  name              = "${local.gold_prefixo}-normaliza-pedidos"
  role_arn          = var.labrole_arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  execution_class   = "STANDARD"
  timeout           = 10
  max_retries       = 0
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_object.script_normalizacao.bucket}/${aws_s3_object.script_normalizacao.key}"
  }
  execution_property {
    max_concurrent_runs = 1
  }
  default_arguments = {
    "--job-language"        = "python"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--RAW_PATH"            = "s3://${terraform_data.bucket_raw.output.bucket}/pedidos/"
    "--GOLD_PATH"           = "s3://${terraform_data.bucket_gold.output.bucket}/"
    "--DDB_TABLE"           = aws_dynamodb_table.execucoes.name
    "--DATASET_NAME"        = "pedidos_desnormalizado"
    "--TempDir"             = "s3://${terraform_data.bucket_gold.output.bucket}/tmp/"
    "--conf"                = "spark.sql.shuffle.partitions=4"
  }
  tags = var.tags
}

resource "aws_glue_catalog_database" "gold" {
  name = "prova_bigdata_6325032"
  tags = var.tags
}

# Esquemas explícitos: dispensa Crawler e seu custo. Depois do job, executar MSCK.
resource "aws_glue_catalog_table" "gold" {
  for_each      = local.gold_tabelas
  name          = each.key
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }
  storage_descriptor {
    location      = "s3://${terraform_data.bucket_gold.output.bucket}/${each.key}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }
    dynamic "columns" {
      for_each = each.value
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
  dynamic "partition_keys" {
    for_each = each.key == "fato_pedidos" ? ["data_pedido"] : []
    content {
      name = partition_keys.value
      type = "string"
    }
  }
}

resource "aws_athena_workgroup" "gold" {
  name          = local.gold_prefixo
  force_destroy = true
  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    bytes_scanned_cutoff_per_query     = 10485760
    result_configuration {
      output_location = "s3://${terraform_data.bucket_gold.output.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
  tags = var.tags
}

output "glue_job_nome" {
  value = aws_glue_job.normaliza_pedidos.name
}
output "athena_database" {
  value = aws_glue_catalog_database.gold.name
}
output "athena_workgroup" {
  value = aws_athena_workgroup.gold.name
}
output "dynamodb_tabela" {
  value = aws_dynamodb_table.execucoes.name
}
output "gold_uri" {
  value = "s3://${terraform_data.bucket_gold.output.bucket}/"
}
