# gold.tf — CAMADA GOLD (construída pelo aluno)
# =============================================================================
# Mesmo padrão do raw.tf: buckets via AWS CLI (terraform_data + local-exec),
# porque a SCP do Learner Lab nega a leitura que o aws_s3_bucket faz.
# =============================================================================

# Bucket_Gold: cria o bucket e deixa PRIVADO (4 bloqueios). Sem upload de CSV:
# os dados do gold são gravados pelo Glue Job.
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
    CMD
  }

  # DESTROY: esvazia (Parquet + script) e remove o Bucket_Gold no terraform destroy.
  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}

# Script do Glue Job: sobe para s3://<gold>/scripts/. O filemd5 no trigger faz o
# Terraform subir de novo sempre que o conteúdo do .py mudar.
resource "terraform_data" "script_glue" {
  triggers_replace = [
    var.bucket_gold_nome,
    filemd5("${path.module}/../glue-job/normaliza_pedidos.py"),
  ]

  provisioner "local-exec" {
    command = <<-CMD
      aws s3 cp "${path.module}/../glue-job/normaliza_pedidos.py" \
        "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    CMD
  }

  depends_on = [terraform_data.bucket_gold]
}

# Catálogo de metadados das execuções (NoSQL). Só a chave é declarada: os outros
# atributos (data_hora, dataset, linhas_*, status) não precisam de esquema fixo.
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

# Glue Job PySpark: lê o raw, normaliza, grava Parquet no gold e metadados no DynamoDB.
# Configuração econômica: menor worker (G.1X), mínimo de workers (2) e timeout curto.
resource "aws_glue_job" "normaliza_pedidos" {
  name              = "normaliza-pedidos"
  role_arn          = var.labrole_arn
  glue_version      = "5.0" # Spark 3.5, igual ao PySpark 3.5 dos testes locais
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 10 # minutos

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
    python_version  = "3"
  }

  default_arguments = {
    "--RAW_PATH"     = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"    = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"    = aws_dynamodb_table.execucoes.name
    "--DATASET_NAME" = "pedidos_desnormalizado"
  }

  tags = var.tags

  # O script precisa estar no S3 antes do Job existir (dependência explícita).
  depends_on = [terraform_data.script_glue]
}

# Glue Data Catalog: database onde ficam as tabelas do gold (sem hífen no nome).
resource "aws_glue_catalog_database" "gold" {
  name = "prova_bigdata"
}

# Crawler: lê os Parquets do gold e cria/atualiza as 3 tabelas (inclusive as
# partições data_pedido=...). Um s3_target por tabela: não varre scripts/ nem
# athena-results/. Roda DEPOIS do Glue Job (aws glue start-crawler).
resource "aws_glue_crawler" "gold" {
  name          = "prova-bigdata-gold-crawler"
  role          = var.labrole_arn
  database_name = aws_glue_catalog_database.gold.name

  s3_target {
    path = "s3://${var.bucket_gold_nome}/fato_pedidos/"
  }

  s3_target {
    path = "s3://${var.bucket_gold_nome}/dim_cliente/"
  }

  s3_target {
    path = "s3://${var.bucket_gold_nome}/dim_produto/"
  }

  tags = var.tags

  depends_on = [terraform_data.bucket_gold]
}

# Athena Workgroup: resultados das consultas em s3://<gold>/athena-results/.
resource "aws_athena_workgroup" "prova" {
  name          = "prova-bigdata"
  force_destroy = true # permite o destroy mesmo com histórico de consultas

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  tags = var.tags

  depends_on = [terraform_data.bucket_gold]
}
