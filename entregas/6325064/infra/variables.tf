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

# ---------------------------------------------------------------------------
# Variáveis novas — camada gold (aluno)
# ---------------------------------------------------------------------------

variable "bucket_gold_nome" {
  description = "Nome global único do bucket S3 gold (dados normalizados em Parquet)."
  type        = string

  validation {
    condition     = length(var.bucket_gold_nome) >= 3 && length(var.bucket_gold_nome) <= 63
    error_message = "O nome do bucket S3 deve ter entre 3 e 63 caracteres."
  }
}

variable "labrole_arn" {
  description = "ARN da LabRole pré-provisionada do Learner Lab (arn:aws:iam::<conta>:role/LabRole)."
  type        = string

  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/LabRole$", var.labrole_arn))
    error_message = "labrole_arn deve ter o formato arn:aws:iam::<12 dígitos>:role/LabRole."
  }
}

variable "tags" {
  description = "Tags de custo padronizadas aplicadas aos recursos que suportam tags."
  type        = map(string)
  default = {
    Projeto    = "ProvaBigData"
    Disciplina = "BigData"
    Ambiente   = "prova"
  }
}