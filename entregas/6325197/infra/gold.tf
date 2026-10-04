# Camada gold — entrega RA 6325197

resource "terraform_data" "bucket_gold" {
  input = {
    bucket = var.bucket_gold_nome
    regiao = var.regiao
  }

  triggers_replace = [var.bucket_gold_nome, var.regiao]

  provisioner "local-exec" {
    environment = {
      GOLD_BUCKET = var.bucket_gold_nome
      AWS_REGION  = var.regiao
    }

    command = <<-CMD
      set -e
      aws s3api create-bucket --bucket "$GOLD_BUCKET" --region "$AWS_REGION"
      aws s3api put-public-access-block \
        --bucket "$GOLD_BUCKET" --region "$AWS_REGION" \
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
    CMD
  }

  provisioner "local-exec" {
    when = destroy

    environment = {
      GOLD_BUCKET = self.input.bucket
      AWS_REGION  = self.input.regiao
    }

    command = "aws s3 rb \"s3://$GOLD_BUCKET\" --force --region \"$AWS_REGION\""
  }
}
