locals {
  buckets_gold = {
    gold       = var.bucket_gold_nome
    resultados = var.bucket_resultados_nome
  }

  tagging_shorthand = "TagSet=[${join(",", [for k, v in var.tags : "{Key=${k},Value=${v}}"])}]"
}

resource "terraform_data" "bucket_gold" {
  for_each = local.buckets_gold

  input = {
    bucket = each.value
    regiao = var.regiao
  }

  triggers_replace = [
    each.value,
    var.regiao,
    local.tagging_shorthand
  ]

  provisioner "local-exec" {
    interpreter = ["PowerShell", "-Command"]
    command     = <<-CMD
      aws s3api create-bucket --bucket ${each.value} --region ${var.regiao} 2>$null
      aws s3api put-public-access-block --bucket ${each.value} --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
      aws s3api put-bucket-tagging --bucket ${each.value} --tagging "${local.tagging_shorthand}"
    CMD
  }

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["PowerShell", "-Command"]
    command     = "aws s3 rb s3://${self.input.bucket} --force 2>$null"
  }
}
