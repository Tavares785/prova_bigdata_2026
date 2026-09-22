# versions.tf — infra (prova)
# PRONTO — não alterar.
#
# IMPORTANTE (AWS Academy Learner Lab): provider aws FIXADO em 5.31.0. Versões
# mais novas fazem, no refresh de S3, chamadas (ex.: GetBucketObjectLockConfiguration)
# que a SCP da organização do Academy NEGA. Além disso, os buckets são criados
# via AWS CLI (ver raw.tf), e não com o recurso aws_s3_bucket.
# Backend local: o state fica na própria pasta (terraform.tfstate).

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.31.0"
    }
  }

  backend "local" {
    path = "terraform.tfstate"
  }
}
