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
# Variáveis da camada gold (declaradas pelo aluno).
# ---------------------------------------------------------------------------

# ARN da LabRole do Learner Lab — usada pelo Glue (sem criar roles/policies).
variable "labrole_arn" {
  description = "ARN da LabRole do Learner Lab (arn:aws:iam::<conta>:role/LabRole)."
  type        = string

  validation {
    condition     = startswith(var.labrole_arn, "arn:aws:iam::")
    error_message = "O labrole_arn deve ser um ARN de role IAM (arn:aws:iam::<conta>:role/LabRole)."
  }
}

# Nome global único do Bucket_Gold (camada gold: Parquet + script do Glue).
variable "bucket_gold_nome" {
  description = "Nome global único do bucket S3 gold (saída Parquet e script do Glue)."
  type        = string

  validation {
    condition     = length(var.bucket_gold_nome) >= 3 && length(var.bucket_gold_nome) <= 63
    error_message = "O nome do bucket S3 deve ter entre 3 e 63 caracteres."
  }
}

# Tags de custo padronizadas (Projeto, Disciplina, Ambiente) — aplicadas com tags = var.tags.
variable "tags" {
  description = "Tags de custo padronizadas aplicadas nos recursos que suportam tags."
  type        = map(string)
}
