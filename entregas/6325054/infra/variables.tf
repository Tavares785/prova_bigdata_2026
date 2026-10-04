# variables.tf — infra (entrega 6325054)
# Declara todas as variáveis usadas pelas camadas raw (raw.tf) e gold (gold.tf).
# Preencha os valores em terraform.tfvars (nunca versione credenciais).

# ---------------------------------------------------------------------------
# Variáveis compartilhadas (raw + gold)
# ---------------------------------------------------------------------------

variable "regiao" {
  description = "Região AWS obrigatória do Learner Lab. Mantenha us-east-1."
  type        = string
  default     = "us-east-1"

  validation {
    condition     = var.regiao == "us-east-1"
    error_message = "O AWS Academy Learner Lab exige a região us-east-1."
  }
}

# ---------------------------------------------------------------------------
# Camada raw
# ---------------------------------------------------------------------------

variable "bucket_raw_nome" {
  description = "Nome global único do bucket S3 raw (camada de origem, com o dataset)."
  type        = string

  validation {
    condition     = length(var.bucket_raw_nome) >= 3 && length(var.bucket_raw_nome) <= 63
    error_message = "O nome do bucket S3 deve ter entre 3 e 63 caracteres."
  }
}

# ---------------------------------------------------------------------------
# Camada gold
# ---------------------------------------------------------------------------

variable "bucket_gold_nome" {
  description = "Nome global único do bucket S3 gold (saída Parquet particionado)."
  type        = string

  validation {
    condition     = length(var.bucket_gold_nome) >= 3 && length(var.bucket_gold_nome) <= 63
    error_message = "O nome do bucket S3 deve ter entre 3 e 63 caracteres."
  }
}

# labrole_arn não é necessária como variável: o gold.tf busca a LabRole via
# data "aws_iam_role" { name = "LabRole" }, que resolve o ARN automaticamente
# a partir da conta autenticada. Não é preciso informar o ARN manualmente.

variable "glue_job_nome" {
  description = "Nome do Glue Job de normalização."
  type        = string
  default     = "normaliza-pedidos"
}

variable "glue_database_nome" {
  description = "Nome do banco de dados no Glue Data Catalog."
  type        = string
  default     = "prova_bigdata_gold"
}

variable "athena_workgroup_nome" {
  description = "Nome do Athena Workgroup."
  type        = string
  default     = "prova-bigdata-wg"
}

variable "dynamodb_table_nome" {
  description = "Nome da tabela DynamoDB de catálogo de execuções."
  type        = string
  default     = "execucoes"
}

# ---------------------------------------------------------------------------
# Tags de custo padronizadas (Req 10.3)
# ---------------------------------------------------------------------------

variable "tags" {
  description = "Tags de custo obrigatórias aplicadas a todos os recursos que suportam tags."
  type        = map(string)
  default = {
    Projeto    = "prova-bigdata"
    Disciplina = "Big Data UNIFAAT"
    Ambiente   = "prova"
  }
}
