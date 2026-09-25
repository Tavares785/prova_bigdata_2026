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
# Camada gold, Glue, Athena e DynamoDB (construídos pelo aluno)
# ---------------------------------------------------------------------------

variable "labrole_arn" {
  description = "ARN da LabRole do Learner Lab (usada pelo Glue Job). Ex.: arn:aws:iam::<conta>:role/LabRole."
  type        = string

  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/.+$", var.labrole_arn))
    error_message = "labrole_arn deve ser um ARN de role IAM (arn:aws:iam::<conta>:role/LabRole)."
  }
}

variable "bucket_gold_nome" {
  description = "Nome global único do bucket S3 gold (Parquet + script do Glue)."
  type        = string

  validation {
    condition     = length(var.bucket_gold_nome) >= 3 && length(var.bucket_gold_nome) <= 63
    error_message = "O nome do bucket S3 deve ter entre 3 e 63 caracteres."
  }
}

variable "bucket_resultados_nome" {
  description = "Nome global único do bucket S3 de resultados de consultas do Athena."
  type        = string

  validation {
    condition     = length(var.bucket_resultados_nome) >= 3 && length(var.bucket_resultados_nome) <= 63
    error_message = "O nome do bucket S3 deve ter entre 3 e 63 caracteres."
  }
}

variable "tags" {
  description = "Tags de custo padronizadas aplicadas a todos os recursos que aceitam tags."
  type        = map(string)
  default = {
    Projeto    = "prova-bigdata"
    Disciplina = "big-data"
    Ambiente   = "lab"
  }
}

variable "dynamodb_tabela_nome" {
  description = "Nome da tabela DynamoDB de catálogo das execuções (repassado ao Job em --DDB_TABLE)."
  type        = string
  default     = "execucoes"
}

variable "glue_job_nome" {
  description = "Nome do Glue Job de normalização."
  type        = string
  default     = "normaliza-pedidos"
}

variable "glue_database_nome" {
  description = "Nome do database no Glue Data Catalog."
  type        = string
  default     = "prova_bigdata"
}

variable "athena_workgroup_nome" {
  description = "Nome do Athena Workgroup da prova."
  type        = string
  default     = "prova-bigdata"
}

variable "dataset_nome" {
  description = "Nome do dataset processado (repassado ao Job em --DATASET_NAME)."
  type        = string
  default     = "pedidos_desnormalizado"
}
