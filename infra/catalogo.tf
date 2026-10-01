# catalogo.tf — Glue Data Catalog (3 tabelas) + Athena Workgroup.
# Partições do fato: partition projection (custo 0, sem Crawler nem MSCK REPAIR). O intervalo de datas
# precisa cobrir o dataset (jan/2026); datas fora dele ficam invisíveis ao Athena.

locals {
  parquet_input  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
  parquet_output = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
  parquet_serde  = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

  colunas = {
    dim_cliente = [
      { nome = "cliente_id", tipo = "string" },
      { nome = "cliente_nome", tipo = "string" },
      { nome = "cliente_uf", tipo = "string" },
    ]
    dim_produto = [
      { nome = "produto_id", tipo = "string" },
      { nome = "produto_nome", tipo = "string" },
      { nome = "categoria", tipo = "string" },
    ]
  }
}

resource "aws_glue_catalog_database" "prova" {
  name = var.glue_database_nome
  tags = var.tags
}

resource "aws_glue_catalog_table" "dim" {
  for_each = local.colunas

  name          = each.key
  database_name = aws_glue_catalog_database.prova.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/${each.key}/"
    input_format  = local.parquet_input
    output_format = local.parquet_output

    ser_de_info {
      serialization_library = local.parquet_serde
    }

    dynamic "columns" {
      for_each = each.value
      content {
        name = columns.value.nome
        type = columns.value.tipo
      }
    }
  }
}

resource "aws_glue_catalog_table" "fato_pedidos" {
  name          = "fato_pedidos"
  database_name = aws_glue_catalog_database.prova.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                        = "TRUE"
    classification                  = "parquet"
    "projection.enabled"            = "true"
    "projection.data_pedido.type"   = "date"
    "projection.data_pedido.format" = "yyyy-MM-dd"
    "projection.data_pedido.range"  = "2026-01-01,2026-12-31"
    "storage.location.template"     = "s3://${var.bucket_gold_nome}/fato_pedidos/data_pedido=$${data_pedido}"
  }

  partition_keys {
    name = "data_pedido"
    type = "date"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_gold_nome}/fato_pedidos/"
    input_format  = local.parquet_input
    output_format = local.parquet_output

    ser_de_info {
      serialization_library = local.parquet_serde
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

# Workgroup com local de resultados e teto de bytes escaneados por consulta (100 MB).
resource "aws_athena_workgroup" "prova" {
  name          = var.athena_workgroup_nome
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true

    bytes_scanned_cutoff_per_query = 104857600

    result_configuration {
      output_location = "s3://${var.bucket_resultados_nome}/"
    }
  }

  tags = var.tags

  depends_on = [terraform_data.bucket]
}
