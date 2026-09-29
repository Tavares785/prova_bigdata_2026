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

  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force || true"
  }
}

# Envio do script do Glue Job para a pasta /scripts/ do bucket Gold
resource "terraform_data" "upload_glue_script" {
  # O upload só acontece DEPOIS que o bucket gold for criado
  depends_on = [terraform_data.bucket_gold]


  triggers_replace = [
    filemd5("${path.module}/../glue-job/normaliza_pedidos.py"),
    var.bucket_gold_nome
  ]

  provisioner "local-exec" {
    # aws s3 cp copiando do diretório local para o S3
    command = "aws s3 cp ${path.module}/../glue-job/normaliza_pedidos.py s3://${var.bucket_gold_nome}/scripts/normaliza_pedidos.py"
  }
}