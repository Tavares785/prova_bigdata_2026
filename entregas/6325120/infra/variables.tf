# ==========================================
# variables.tf — infra (prova)
# ==========================================

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

# ==========================================
# VARIÁVEIS ADICIONADAS PARA A CAMADA GOLD
# ==========================================

variable "bucket_gold_nome" {
  description = "Nome global único do bucket S3 gold (camada de destino)."
  type        = string

  validation {
    condition     = length(var.bucket_gold_nome) >= 3 && length(var.bucket_gold_nome) <= 63
    error_message = "O nome do bucket S3 deve ter entre 3 e 63 caracteres."
  }
}

variable "labrole_arn" {
  description = "ARN da LabRole do AWS Academy Learner Lab."
  type        = string
}

variable "tags" {
  description = "Tags padronizadas de custos e identificação dos recursos."
  type        = map(string)
  default     = {
    Ambiente = "Producao"
    Projeto  = "Prova_AWS"
  }
}