# raw.tf — CAMADA RAW  ✅ PRONTO (não altere)
# =============================================================================
# Esta parte vem PRONTA. Ela cria o Bucket_Raw (privado) e sobe o
# Dataset_Exemplo desnormalizado — simulando a camada de origem que, no mundo
# real, já existiria. Você (aluno) NÃO precisa mexer aqui; foque no gold.tf.
#
# ⚠️ Learner Lab: os buckets NÃO são criados com aws_s3_bucket (a SCP da
# organização NEGA s3:GetBucketObjectLockConfiguration, que o recurso
# aws_s3_bucket sempre lê -> AccessDenied). Por isso criamos via AWS CLI dentro
# do `terraform apply` (terraform_data + local-exec). REQUER o AWS CLI instalado.
# Requirements: 3.1, 3.2, 3.3, 3.6, 11.1.
# =============================================================================

# Provider AWS — PRONTO. Região fixada em var.regiao (us-east-1).
provider "aws" {
  region = var.regiao
}

# Bucket_Raw (camada raw) criado via CLI: cria o bucket, deixa PRIVADO (4
# bloqueios) e sobe o Dataset_Exemplo para s3://<raw>/pedidos/ durante o apply.
resource "terraform_data" "bucket_raw" {
  input = {
    bucket = var.bucket_raw_nome
    regiao = var.regiao
  }

  triggers_replace = [var.bucket_raw_nome, var.regiao]

  provisioner "local-exec" {
    command = <<-CMD
      set -e
      aws s3api create-bucket --bucket "${var.bucket_raw_nome}" --region "${var.regiao}" 2>/dev/null || true
      aws s3api put-public-access-block --bucket "${var.bucket_raw_nome}" \
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
      aws s3 cp "${path.module}/../dataset/pedidos_desnormalizado.csv" \
        "s3://${var.bucket_raw_nome}/pedidos/pedidos_desnormalizado.csv" --content-type text/csv
    CMD
  }

  # DESTROY: esvazia e remove o Bucket_Raw no terraform destroy.
  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}
