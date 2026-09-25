# Camada gold: armazenamento, processamento, catálogo, consulta e metadados.

resource "terraform_data" "bucket_gold" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }

  triggers_replace = [var.bucket_gold_nome, var.regiao]

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = "aws s3api create-bucket --bucket \"${var.bucket_gold_nome}\" --region \"${var.regiao}\" 2>/dev/null || true && aws s3api put-public-access-block --bucket \"${var.bucket_gold_nome}\" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
  }

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["/bin/bash", "-c"]
    command     = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}

resource "terraform_data" "glue_script" {
  input = {
    bucket = var.bucket_gold_nome
    script = filemd5("${path.module}/../glue-job/normaliza_pedidos.py")
  }

  depends_on = [terraform_data.bucket_gold]

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = "aws s3 cp \"${path.module}/../glue-job/normaliza_pedidos.py\" \"s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py\" --region \"${var.regiao}\""
  }
}

resource "aws_glue_catalog_database" "gold" {
  name = "prova_bigdata_gold"
}

resource "aws_glue_job" "normaliza_pedidos" {
  name              = "normaliza-pedidos"
  role_arn          = var.labrole_arn
  glue_version      = "4.0"
  max_retries       = 0
  timeout           = 10
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--RAW_PATH"                         = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"                        = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"                        = aws_dynamodb_table.execucoes.name
    "--DATASET_NAME"                     = "pedidos_desnormalizado"
  }

  tags       = var.tags
  depends_on = [terraform_data.glue_script]
}

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

resource "aws_athena_workgroup" "gold" {
  name = "prova-bigdata"

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  tags = var.tags
}

resource "aws_glue_catalog_table" "fato_pedidos" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification = "parquet"
    type           = "parquet"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/fato_pedidos/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

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
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification = "parquet"
    type           = "parquet"
  }

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
  parameters = {
    classification = "parquet"
    type           = "parquet"
  }

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
