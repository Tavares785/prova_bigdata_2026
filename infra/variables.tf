variable "lab_role_arn" {
  description = "ARN da role do IAM para o AWS Glue"
  type        = string
}

variable "bucket_raw_nome" {
  description = "Nome do bucket S3 da camada Raw"
  type        = string
}

variable "bucket_gold_nome" {
  description = "Nome do bucket S3 da camada Gold"
  type        = string
}

variable "regiao" {
  description = "Regiao da AWS"
  type        = string
  default     = "us-east-1"
}
