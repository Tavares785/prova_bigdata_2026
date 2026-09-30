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

# ─────────────────────────────────────────────────────────────────────────────
# Variáveis da camada gold — ALUNO preenche / declara.
# ─────────────────────────────────────────────────────────────────────────────

variable "labrole_arn" {
  description = "ARN da LabRole pré-provisionada no Learner Lab. Não crie roles/policies próprias."
  type        = string

  validation {
    condition     = can(regex("^arn:aws:iam::\\d+:role/.+", var.labrole_arn))
    error_message = "Informe um ARN válido de IAM Role (arn:aws:iam::<account-id>:role/<nome>)."
  }
}

variable "bucket_gold_nome" {
  description = "Nome global único do bucket S3 gold (camada normalizada, Parquet)."
  type        = string

  validation {
    condition     = length(var.bucket_gold_nome) >= 3 && length(var.bucket_gold_nome) <= 63
    error_message = "O nome do bucket S3 deve ter entre 3 e 63 caracteres."
  }
}

variable "glue_job_nome" {
  description = "Nome do Glue Job de normalização."
  type        = string
  default     = "normaliza-pedidos"
}

variable "dynamodb_tabela_nome" {
  description = "Nome da tabela DynamoDB de catálogo de execuções."
  type        = string
  default     = "execucoes"
}

variable "athena_workgroup_nome" {
  description = "Nome do Athena Workgroup."
  type        = string
  default     = "prova-bigdata-wg"
}

variable "glue_database_nome" {
  description = "Nome do banco de dados no Glue Data Catalog."
  type        = string
  default     = "prova_bigdata_gold"
}

variable "tags" {
  description = "Tags de custo padronizadas aplicadas a todos os recursos."
  type        = map(string)
  default = {
    Projeto    = "prova-bigdata"
    Disciplina = "Big Data"
    Ambiente   = "dev"
  }
}
