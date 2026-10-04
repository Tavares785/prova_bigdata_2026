# versions.tf — infra (entrega 6325054)
# Idêntico ao versions.tf do professor: provider AWS fixado em 5.31.0 para
# evitar chamadas bloqueadas pela SCP do Learner Lab (ex.: GetBucketObjectLockConfiguration).
# Backend local: state na própria pasta.

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
