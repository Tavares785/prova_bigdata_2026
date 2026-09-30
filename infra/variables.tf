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

# Nome global único do Bucket_Gold (camada gold — o aluno cria).
variable "bucket_gold_nome" {
  description = "Nome global único do bucket S3 gold (destino dos dados normalizados em Parquet)."
  type        = string

  validation {
    condition     = length(var.bucket_gold_nome) >= 3 && length(var.bucket_gold_nome) <= 63
    error_message = "O nome do bucket S3 deve ter entre 3 e 63 caracteres."
  }
}

# ARN da LabRole fornecida pelo AWS Academy Learner Lab.
variable "labrole_arn" {
  description = "ARN da LabRole do Learner Lab. Usada pelo Glue Job sem criar roles próprias."
  type        = string
}

# Nome da tabela DynamoDB de metadados de execução.
variable "dynamodb_table_nome" {
  description = "Nome da tabela DynamoDB que registra os metadados de cada execução do Glue Job."
  type        = string
}

# Nome do Glue Job.
variable "glue_job_nome" {
  description = "Nome do Glue Job de normalização."
  type        = string
}

# Tags de custo obrigatórias aplicadas a todos os recursos.
variable "tags" {
  description = "Tags de custo padronizadas (Projeto, Disciplina, Ambiente) aplicadas a todos os recursos."
  type        = map(string)
  default = {
    Projeto    = "prova-bigdata"
    Disciplina = "Big Data"
    Ambiente   = "producao"
  }
}
