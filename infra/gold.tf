resource "aws_dynamodb_table" "execucoes" {
  name         = var.dynamodb_tabela_nome
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = {
    Name = "execucoes"
  }
}

data "aws_caller_identity" "current" {}

resource "aws_glue_job" "etl" {
  name     = var.glue_job_nome
  role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/LabRole"

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/glue-job/normaliza_pedidos.py"
    python_version  = "3"
  }

  default_arguments = {
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"    = var.dynamodb_tabela_nome
    "--DATASET_NAME" = "pedidos_desnormalizado"
    "--job-language" = "python"
  }

  depends_on = [
    terraform_data.bucket_gold
  ]
}

resource "aws_glue_catalog_database" "analytics" {
  name = "analytics"
}

resource "aws_glue_catalog_table" "dim_cliente" {
  name          = "dim_cliente"
  database_name = aws_glue_catalog_database.analytics.name
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
}

resource "aws_glue_catalog_table" "dim_produto" {
  name          = "dim_produto"
  database_name = aws_glue_catalog_database.analytics.name
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
}

resource "aws_glue_catalog_table" "fato_pedidos" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.analytics.name
  table_type    = "EXTERNAL_TABLE"

  partition_keys {
    name = "data_pedido"
    type = "date"
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

  depends_on = [
    terraform_data.bucket_gold
  ]
}

resource "terraform_data" "bucket_gold" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }

  triggers_replace = [var.bucket_gold_nome, var.regiao]

  provisioner "local-exec" {
    interpreter = ["C:/Program Files/Git/bin/bash.exe", "-c"]
    command     = <<-CMD
      set -e
      aws s3api create-bucket --bucket "${var.bucket_gold_nome}" --region "${var.regiao}" 2>/dev/null || true
      aws s3api put-public-access-block --bucket "${var.bucket_gold_nome}" \
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
    CMD
  }

  provisioner "local-exec" {
    interpreter = ["C:/Program Files/Git/bin/bash.exe", "-c"]
    when        = destroy
    command     = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}
