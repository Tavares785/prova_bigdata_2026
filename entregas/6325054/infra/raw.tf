# raw.tf — CAMADA RAW (entrega 6325054)
# =============================================================================
# Reproduz o raw.tf do professor sem alterações de lógica.
# Cria o Bucket_Raw (privado) via AWS CLI e sobe o dataset desnormalizado.
#
# ⚠️ Learner Lab: NÃO usa aws_s3_bucket (a SCP nega GetBucketObjectLockConfiguration).
#    O bucket é criado via AWS CLI dentro do terraform apply (terraform_data + local-exec).
#    REQUER AWS CLI v2 instalado e autenticado.
# =============================================================================

# Provider AWS — região fixada em var.regiao (us-east-1).
provider "aws" {
  region = var.regiao
}

# Bucket_Raw criado via CLI: privado (4 bloqueios) + upload do dataset durante o apply.
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
