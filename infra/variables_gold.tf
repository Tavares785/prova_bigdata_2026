variable "bucket_gold_nome" {
  description = "Nome globalmente unico do bucket S3 da camada GOLD."
  type        = string

  validation {
    condition = (
      length(var.bucket_gold_nome) >= 3 &&
      length(var.bucket_gold_nome) <= 63 &&
      can(regex("^[a-z0-9][a-z0-9.-]*[a-z0-9]$", var.bucket_gold_nome))
    )
    error_message = "O nome do bucket deve ter entre 3 e 63 caracteres, usar apenas letras minusculas, numeros, pontos ou hifens, e comecar/terminar com letra ou numero."
  }
}

variable "dynamodb_tabela_nome" {
  description = "Nome da tabela DynamoDB que guarda os metadados das execucoes do Glue Job."
  type        = string

  validation {
    condition = (
      length(var.dynamodb_tabela_nome) >= 3 &&
      length(var.dynamodb_tabela_nome) <= 255 &&
      can(regex("^[a-zA-Z0-9_.-]+$", var.dynamodb_tabela_nome))
    )
    error_message = "O nome da tabela DynamoDB deve ter entre 3 e 255 caracteres, usando apenas letras, numeros, underscore, ponto ou hifen."
  }
}

variable "glue_job_nome" {
  description = "Nome do Glue Job que executa a normalizacao dos pedidos."
  type        = string

  validation {
    condition     = length(var.glue_job_nome) > 0 && length(var.glue_job_nome) <= 255
    error_message = "O nome do Glue Job nao pode ser vazio e deve ter no maximo 255 caracteres."
  }
}