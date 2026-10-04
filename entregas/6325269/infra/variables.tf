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

# --- Camada gold (aluno) ---

# ARN da LabRole do Learner Lab, usada pelo Glue para acessar S3, DynamoDB etc.
variable "labrole_arn" {
  description = "ARN da LabRole do AWS Academy (ex.: arn:aws:iam::123456789012:role/LabRole)."
  type        = string

  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/LabRole$", var.labrole_arn))
    error_message = "Use um ARN de role IAM válido: arn:aws:iam::<12 dígitos>:role/LabRole."
  }
}

# Nome global único do Bucket_Gold (camada gold, dados tratados).
variable "bucket_gold_nome" {
  description = "Nome global único do bucket S3 gold (camada de dados tratados)."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.bucket_gold_nome))
    error_message = "O bucket deve ter 3 a 63 caracteres, só minúsculas, dígitos e hífen, começando e terminando com letra ou dígito."
  }
}

# Tags obrigatórias aplicadas a todos os recursos da camada gold.
variable "tags" {
  description = "Tags comuns dos recursos (precisam conter Projeto, Disciplina e Ambiente)."
  type        = map(string)
  default = {
    Projeto    = "prova-bigdata"
    Disciplina = "BigData"
    Ambiente   = "learner-lab"
  }

  validation {
    condition     = alltrue([for k in ["Projeto", "Disciplina", "Ambiente"] : contains(keys(var.tags), k)])
    error_message = "As tags precisam conter as chaves Projeto, Disciplina e Ambiente."
  }
}

# Nome do job do Glue que transforma raw em gold.
variable "glue_job_nome" {
  description = "Nome do job ETL do AWS Glue (raw -> gold)."
  type        = string
  default     = "job-gold-6325269"
}

# Tabela DynamoDB com os metadados de cada execução do Glue Job.
variable "dynamodb_tabela_nome" {
  description = "Tabela DynamoDB com os metadados de cada execução do Glue Job."
  type        = string
  default     = "execucoes-6325269"
}

# Nome do database do Glue Data Catalog (use underscore, não hífen, por causa do Athena).
variable "glue_database_nome" {
  description = "Nome do database no Glue Data Catalog."
  type        = string
  default     = "gold_6325269"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9_]{1,61}[a-z0-9]$", var.glue_database_nome))
    error_message = "Use só minúsculas, dígitos e underscore, começando e terminando com letra ou dígito."
  }
}

# Nome do workgroup do Athena para as consultas na camada gold.
variable "athena_workgroup_nome" {
  description = "Nome do workgroup do Amazon Athena."
  type        = string
  default     = "wg-gold-6325269"
}

# Nome do dataset gravado no campo dataset dos metadados do DynamoDB.
variable "dataset_nome" {
  description = "Nome do dataset gravado no campo dataset dos metadados do DynamoDB."
  type        = string
  default     = "pedidos_desnormalizado"
}
