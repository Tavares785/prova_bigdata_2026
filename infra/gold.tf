# gold.tf — CAMADA GOLD (construída pelo aluno)
# =============================================================================
# Provisiona, no mesmo `terraform apply` da raw:
#   - Bucket_Gold privado (via AWS CLI, mesmo padrão do raw.tf)
#   - Upload do script do Glue Job
#   - Tabela DynamoDB (catálogo de metadados das execuções)
#   - Glue Job PySpark (glueetl) usando a LabRole
#   - Glue Data Catalog (database + Crawler) para o Athena enxergar o gold
#   - Athena Workgroup com local de resultados
#
# Learner Lab: NÃO cria roles/policies — usa a LabRole existente.
# =============================================================================

# LabRole buscada pelo nome: o ARN se adapta a qualquer conta do Learner Lab,
# sem precisar preencher nada no terraform.tfvars.
data "aws_iam_role" "labrole" {
  name = "LabRole"
}

# Bucket_Gold criado via CLI (mesmo padrão do raw.tf): a SCP do Learner Lab
# nega s3:GetBucketObjectLockConfiguration, que o recurso aws_s3_bucket sempre
# lê. Cria o bucket, deixa PRIVADO (4 bloqueios de acesso público) e aplica
# as tags de custo (put-bucket-tagging), já que não há recurso Terraform com tags.
resource "terraform_data" "bucket_gold" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }

  triggers_replace = [var.bucket_gold_nome, var.regiao, var.tags]

  provisioner "local-exec" {
    command = <<-CMD
      set -e
      aws s3api create-bucket --bucket "${var.bucket_gold_nome}" --region "${var.regiao}" 2>/dev/null || true
      aws s3api put-public-access-block --bucket "${var.bucket_gold_nome}" \
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
      aws s3api put-bucket-tagging --bucket "${var.bucket_gold_nome}" \
        --tagging '${jsonencode({ TagSet = [for k, v in var.tags : { Key = k, Value = v }] })}'
    CMD
  }

  # DESTROY: --force esvazia (Parquet, script, resultados do Athena) e remove o
  # bucket; sem ele o rb falharia com o bucket não vazio.
  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}

# Script do Glue Job publicado no gold (o Glue lê o .py do S3). O etag força
# o re-upload sempre que o script mudar.
resource "aws_s3_object" "script" {
  bucket = var.bucket_gold_nome
  key    = "scripts/normaliza_pedidos.py"
  source = "${path.module}/../glue-job/normaliza_pedidos.py"
  etag   = filemd5("${path.module}/../glue-job/normaliza_pedidos.py")

  # O bucket vem de uma variável (sem referência ao recurso), então a ordem
  # precisa ser explícita: sem isso o upload pode rodar antes do bucket existir.
  depends_on = [terraform_data.bucket_gold]
}

# Catálogo NoSQL de metadados das execuções (1 item por execução do job).
# PAY_PER_REQUEST: sem capacidade reservada, sem custo parado. Só a chave é
# declarada em `attribute`; os demais atributos do item são schemaless.
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

# Glue Job PySpark (glueetl): lê o raw, normaliza, grava Parquet no gold e o
# item de metadados no DynamoDB. Driver/executors sob demanda, sem cluster
# ligado. Usa a LabRole (sem criar roles). Configuração econômica: Glue 4.0,
# G.1X (menor worker padrão), 2 workers (1 driver + 1 executor).
resource "aws_glue_job" "normaliza_pedidos" {
  name     = "normaliza-pedidos"
  role_arn = data.aws_iam_role.labrole.arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 15 # minutos; evita consumo do orçamento se travar
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_object.script.bucket}/${aws_s3_object.script.key}"
  }

  # Os nomes batem com o getResolvedOptions do normaliza_pedidos.py
  # (JOB_NAME é injetado pelo próprio Glue).
  default_arguments = {
    "--RAW_PATH"                         = "s3://${var.bucket_raw_nome}/pedidos/"
    "--GOLD_PATH"                        = "s3://${var.bucket_gold_nome}/"
    "--DDB_TABLE"                        = aws_dynamodb_table.execucoes.name
    "--DATASET_NAME"                     = "pedidos_desnormalizado"
    "--TempDir"                          = "s3://${var.bucket_gold_nome}/tmp/"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  tags = var.tags
}

# Database do Glue Data Catalog onde ficam as tabelas do gold (o Athena lê daqui).
# Nome com underscore: o Athena não aceita hífen em nome de database sem aspas.
resource "aws_glue_catalog_database" "gold" {
  name = "prova_bigdata_gold"

  tags = var.tags
}

# Crawler: descobre o esquema do Parquet e as partições data_pedido=... e
# registra as tabelas. Um alvo por tabela, para não catalogar scripts/, tmp/
# nem athena-results/. Rodar DEPOIS do job: aws glue start-crawler.
resource "aws_glue_crawler" "gold" {
  name          = "crawler-prova-bigdata-gold"
  role          = data.aws_iam_role.labrole.arn
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

# Athena Workgroup: resultados das consultas em um prefixo próprio do gold
# (fora das pastas das tabelas). force_destroy permite o destroy mesmo com
# histórico de consultas no workgroup.
resource "aws_athena_workgroup" "prova" {
  name = "prova-bigdata"

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_gold_nome}/athena-results/"
    }
  }

  force_destroy = true

  tags = var.tags
}
