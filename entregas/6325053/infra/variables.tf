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
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.bucket_raw_nome))
    error_message = "Use de 3 a 63 caracteres minúsculos, números e hífens para o bucket raw."
  }
}

variable "labrole_arn" {
  description = "ARN da LabRole preexistente no Learner Lab."
  type        = string
  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/(.*/)?LabRole$", var.labrole_arn))
    error_message = "Informe o ARN real da LabRole da conta do laboratório."
  }
}

variable "bucket_gold_nome" {
  description = "Bucket exclusivo da entrega, globalmente único."
  type        = string
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.bucket_gold_nome))
    error_message = "Use de 3 a 63 caracteres minúsculos, números e hífens."
  }
}

variable "tags" {
  type = map(string)
  default = {
    Projeto    = "prova-bigdata-6325053"
    Disciplina = "BigData"
    Ambiente   = "LearnerLab"
    RA         = "6325053"
  }
  validation {
    condition     = alltrue([for k in ["Projeto", "Disciplina", "Ambiente"] : try(length(trimspace(var.tags[k])) > 0, false)])
    error_message = "Preencha as tags Projeto, Disciplina e Ambiente."
  }
}
