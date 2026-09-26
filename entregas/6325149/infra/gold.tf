# gold.tf — camada gold (aluno)
# Buckets criados via AWS CLI (terraform_data + local-exec), pelo mesmo motivo do raw.tf: a SCP do
# Learner Lab nega s3:GetBucketObjectLockConfiguration, lido por aws_s3_bucket.
# Diferente do raw.tf, aqui a falha NÃO é escondida (sem `|| true`): apply/destroy que falham aparecem.

locals {
  buckets_cli = {
    gold       = var.bucket_gold_nome
    resultados = var.bucket_resultados_nome
  }

  # Formato esperado por `aws s3api put-bucket-tagging --tagging`.
  tagging_json = jsonencode({
    TagSet = [for chave, valor in var.tags : { Key = chave, Value = valor }]
  })
}

# Bucket_Gold e bucket de resultados do Athena: privados (4 bloqueios) e com tags.
resource "terraform_data" "bucket" {
  for_each = local.buckets_cli

  input = {
    bucket = each.value
    regiao = var.regiao
  }

  triggers_replace = [each.value, var.regiao, local.tagging_json]

  provisioner "local-exec" {
    environment = {
      TAGGING = local.tagging_json
    }
    command = <<-CMD
      set -e
      aws s3api create-bucket --bucket "${each.value}" --region "${var.regiao}"
      aws s3api put-public-access-block --bucket "${each.value}" \
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
      aws s3api put-bucket-tagging --bucket "${each.value}" --tagging "$TAGGING"
    CMD
  }

  # DESTROY: esvazia e remove o bucket. Sem `|| true`: se falhar (ex.: credencial expirada), o destroy falha.
  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force"
  }
}

# Script do Glue Job. Vive no bucket gold, em prefixo próprio (scripts/), fora dos caminhos das tabelas.
resource "terraform_data" "upload_script" {
  input = {
    bucket = var.bucket_gold_nome
  }

  triggers_replace = [filemd5("${path.module}/../glue-job/normaliza_pedidos.py"), var.bucket_gold_nome]

  provisioner "local-exec" {
    command = "aws s3 cp \"${path.module}/../glue-job/normaliza_pedidos.py\" \"s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py\""
  }

  depends_on = [terraform_data.bucket]
}

# Catálogo de metadados das execuções (Aula 04).
resource "aws_dynamodb_table" "execucoes" {
  name         = var.dynamodb_tabela_nome
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = var.tags
}
