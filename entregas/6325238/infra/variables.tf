# variables.tf — infra (prova)
# Variáveis de entrada da camada raw (PRONTA). Preencha os valores em
# terraform.tfvars (ver terraform.tfvars.example).
#
# ✍️ ALUNO: ao construir a sua infraestrutura (gold, Glue, Athena, DynamoDB),
#    você mesmo declara aqui as variáveis novas que precisar (ex.: nome do
#    bucket gold, ARN da LabRole, tags etc.). Só a raw vem pronta.

variable "regiao" {
  description = "Região AWS obrigatória do Learner Lab. Mantenha us-east-1."
  type        = string
  default     = "us-east-1"

  validation {
    condition     = var.regiao == "us-east-1"
    error_message = "O AWS Academy Learner Lab exige a região us-east-1."
  }
}

# Nome global único do Bucket_Raw (camada raw — entra PRONTO, o professor entrega).
variable "bucket_raw_nome" {
  description = "Nome global único do bucket S3 raw (camada de origem, com o dataset)."
  type        = string

  validation {
    condition     = length(var.bucket_raw_nome) >= 3 && length(var.bucket_raw_nome) <= 63
    error_message = "O nome do bucket S3 deve ter entre 3 e 63 caracteres."
  }
}

variable "bucket_gold_nome" {
  description = "Nome globalmente único do bucket S3 da camada gold."
  type        = string

  validation {
    condition     = length(var.bucket_gold_nome) >= 3 && length(var.bucket_gold_nome) <= 63
    error_message = "O nome do bucket S3 deve ter entre 3 e 63 caracteres."
  }
}

variable "lab_role_arn" {
  description = "ARN da LabRole existente no AWS Academy Learner Lab."
  type        = string
}

variable "glue_job_nome" {
  description = "Nome do Glue Job."
  type        = string
  default     = "normaliza-pedidos"
}

variable "dynamodb_tabela_nome" {
  description = "Nome da tabela DynamoDB de execuções."
  type        = string
  default     = "execucoes"
}

variable "athena_database_nome" {
  description = "Nome do database do Glue Data Catalog usado pelo Athena."
  type        = string
  default     = "pedidos"
}

variable "tags" {
  description = "Tags dos recursos."
  type        = map(string)

  default = {
    Projeto    = "ProvaBigData"
    Aluno      = "Yuri Batista Sanches"
    RA         = "6325238"
    Disciplina = "Big Data"
  }
}