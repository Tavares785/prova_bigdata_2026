# gold.tf — CAMADA GOLD (aluno preenche)

# ---------------------------------------------------------------------------
# Variáveis novas — também em variables.tf
# ---------------------------------------------------------------------------
# bucket_gold_nome, labrole_arn, dynamodb_table_nome, glue_job_nome

# ---------------------------------------------------------------------------
# 1. Bucket_Gold (mesmo padrão do raw.tf — terraform_data + local-exec)
# ---------------------------------------------------------------------------
resource "terraform_data" "bucket_gold" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }

  triggers_replace = [var.bucket_gold_nome, var.regiao]

  provisioner "local-exec" {

    interpreter = ["bash", "-c"]
    command     = "aws s3api create-bucket --bucket \"${var.bucket_gold_nome}\" --region \"${var.regiao}\" 2>/dev/null || true && aws s3api put-public-access-block --bucket \"${var.bucket_gold_nome}\" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
  }

  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}

# ---------------------------------------------------------------------------
# 2. DynamoDB — tabela de metadados de execução
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

# ---------------------------------------------------------------------------
# 3. Glue Job
# ---------------------------------------------------------------------------
resource "aws_glue_job" "normalizacao" {
  name     = var.glue_job_nome
  role_arn = var.labrole_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }

  default_arguments = {
    "--RAW_PATH"      = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"     = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"     = var.dynamodb_table_nome
    "--DATASET_NAME"  = "pedidos_desnormalizado"
  }

  tags = var.tags
}

# ---------------------------------------------------------------------------
# 4. Glue Data Catalog — database + 3 tabelas
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_database" "gold" {
  name = "gold"
}

resource "aws_glue_catalog_table" "fato_pedidos" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.gold.name

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/fato_pedidos/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
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

  partition_keys {
    name = "data_pedido"
    type = "date"
  }
}

resource "aws_glue_catalog_table" "dim_cliente" {
  name          = "dim_cliente"
  database_name = aws_glue_catalog_database.gold.name

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/dim_cliente/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

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
  database_name =  aws_glue_catalog_database.gold.name

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/dim_produto/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

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

# ---------------------------------------------------------------------------
# 5. Athena Workgroup
# ---------------------------------------------------------------------------
resource "aws_athena_workgroup" "gold" {
  name = "gold"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  tags = var.tags
}
