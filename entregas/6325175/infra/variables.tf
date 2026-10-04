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
# Variáveis da camada gold — declaradas pelo aluno conforme o design técnico.
# Preencha os valores reais em terraform.tfvars (nunca versionar esse arquivo).
# ---------------------------------------------------------------------------

# Nome global único do Bucket_Gold (camada de destino — Parquet normalizado).
variable "bucket_gold_nome" {
  description = "Nome global único do bucket S3 gold (camada de destino, dados normalizados em Parquet)."
  type        = string

  validation {
    condition     = length(var.bucket_gold_nome) >= 3 && length(var.bucket_gold_nome) <= 63
    error_message = "O nome do bucket S3 deve ter entre 3 e 63 caracteres."
  }
}

# ARN completo da LabRole do AWS Academy Learner Lab.
# Exemplo: arn:aws:iam::123456789012:role/LabRole
variable "labrole_arn" {
  description = "ARN completo da LabRole do AWS Academy Learner Lab (ex.: arn:aws:iam::<account_id>:role/LabRole). Deve ser fornecido pelo aluno via terraform.tfvars."
  type        = string
}

# Tags padronizadas aplicadas a todos os recursos que aceitam tags.
# Sobrescreva os valores de exemplo no terraform.tfvars.
variable "tags" {
  description = "Tags padronizadas de custo e rastreabilidade aplicadas a todos os recursos que aceitam tags."
  type = object({
    Projeto    = string
    Disciplina = string
    Ambiente   = string
  })
  default = {
    Projeto    = "prova-bigdata"
    Disciplina = "big-data"
    Ambiente   = "lab"
  }
}
