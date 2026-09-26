variable "regiao" {
  type    = string
  default = "us-east-1"
  validation {
    condition     = var.regiao == "us-east-1"
    error_message = "O Learner Lab desta prova deve usar us-east-1."
  }
}

variable "bucket_raw_nome" {
  type = string
  validation {
    condition     = can(regex("^prova-bigdata-raw-6325032-[a-z0-9]([a-z0-9-]*[a-z0-9])?$", var.bucket_raw_nome)) && length(var.bucket_raw_nome) <= 63
    error_message = "Use prova-bigdata-raw-6325032-<sufixo-unico>, com no máximo 63 caracteres."
  }
}

variable "bucket_gold_nome" {
  type = string
  validation {
    condition     = can(regex("^prova-bigdata-gold-6325032-[a-z0-9]([a-z0-9-]*[a-z0-9])?$", var.bucket_gold_nome)) && length(var.bucket_gold_nome) <= 63
    error_message = "Use prova-bigdata-gold-6325032-<sufixo-unico>, com no máximo 63 caracteres."
  }
}

variable "labrole_arn" {
  type        = string
  description = "ARN da LabRole existente. Nenhuma role ou policy é criada."
  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/(.*/)?LabRole$", var.labrole_arn))
    error_message = "Informe o ARN da LabRole do seu Learner Lab."
  }
}

variable "tags" {
  type = map(string)
  default = {
    Projeto    = "prova-bigdata-6325032"
    Disciplina = "Big Data - UNIFAAT"
    Ambiente   = "LearnerLab"
    RA         = "6325032"
  }
  validation {
    condition     = alltrue([for nome in ["Projeto", "Disciplina", "Ambiente"] : try(length(trimspace(var.tags[nome])) > 0, false)]) && !contains(keys(var.tags), "TerraformOwner")
    error_message = "Preencha Projeto, Disciplina e Ambiente; TerraformOwner é reservada à identificação do recurso."
  }
}
