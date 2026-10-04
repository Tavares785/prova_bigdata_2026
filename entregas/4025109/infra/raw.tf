# raw.tf — CAMADA RAW  ✅ PRONTO (não altere)
# =============================================================================
# Requirements: 3.1, 3.2, 3.3, 3.6, 11.1.
# =============================================================================

# Provider AWS — PRONTO. Região fixada em var.regiao (us-east-1).
provider "aws" {
  region = var.regiao
}

resource "terraform_data" "bucket_raw" {
  input = {
    bucket = var.bucket_raw_nome
    regiao = var.regiao
  }

  triggers_replace = [var.bucket_raw_nome, var.regiao]

  provisioner "local-exec" {
    interpreter = ["PowerShell", "-Command"]
    command     = <<-CMD
      aws s3api create-bucket --bucket "${var.bucket_raw_nome}" --region "${var.regiao}" 2>$null; `
      aws s3api put-public-access-block --bucket "${var.bucket_raw_nome}" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true; `
      aws s3 cp "${path.module}/../dataset/pedidos_desnormalizado.csv" "s3://${var.bucket_raw_nome}/pedidos/pedidos_desnormalizado.csv" --content-type text/csv
    CMD
  }

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["PowerShell", "-Command"]
    command     = "aws s3 rb s3://${self.input.bucket} --force; exit 0"
  }
}
