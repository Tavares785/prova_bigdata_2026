locals {
  bucket_tags_json = jsonencode({
    TagSet = [
      for chave, valor in var.tags : {
        Key   = chave
        Value = valor
      }
    ]
  })
}

resource "terraform_data" "tags_buckets" {
  for_each = {
    raw  = var.bucket_raw_nome
    gold = var.bucket_gold_nome
  }

  triggers_replace = [
    each.value,
    local.bucket_tags_json,
    terraform_data.bucket_raw.id,
    terraform_data.bucket_gold.id
  ]

  provisioner "local-exec" {
    environment = {
      BUCKET_NAME = each.value
      BUCKET_TAGS = local.bucket_tags_json
      AWS_REGION  = var.regiao
    }

    command = "aws s3api put-bucket-tagging --bucket \"$BUCKET_NAME\" --tagging \"$BUCKET_TAGS\" --region \"$AWS_REGION\""
  }

  depends_on = [
    terraform_data.bucket_raw,
    terraform_data.bucket_gold
  ]
}
