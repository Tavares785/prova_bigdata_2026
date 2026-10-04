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

# Nome global único do Bucket_Gold (camada gold — você constrói).
variable "bucket_gold_nome" {
  description = "Nome global único do bucket S3 gold (camada normalizada, Parquet)."
  type        = string

  validation {
    condition     = length(var.bucket_gold_nome) >= 3 && length(var.bucket_gold_nome) <= 63
    error_message = "O nome do bucket S3 deve ter entre 3 e 63 caracteres."
  }
}

# ARN da LabRole pré-provisionada no Learner Lab.
# Obtenha com: aws iam get-role --role-name LabRole --query "Role.Arn" --output text
variable "labrole_arn" {
  description = "ARN da LabRole do AWS Academy Learner Lab (usada pelo Glue Job)."
  type        = string
}

# Tags de custo padronizadas aplicadas a todos os recursos que suportam tags.
variable "tags" {
  description = "Tags de custo padronizadas (Projeto, Disciplina, Ambiente)."
  type        = map(string)
  default = {
    Projeto    = "prova-bigdata"
    Disciplina = "BigData"
    Ambiente   = "dev"
  }
}
