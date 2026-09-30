# variables.tf — infra (prova)
# Variáveis de entrada da camada raw (PRONTA). Preencha os valores em
# terraform.tfvars (ver terraform.tfvars.example).
#
# ✍️ ALUNO: ao construir a sua infraestrutura (gold, Glue, Athena, DynamoDB),
#    você mesmo declara aqui as variáveis novas que precisar (ex.: nome do
#    bucket gold, ARN da LabRole, tags etc.). Só a raw vem pronta.

variable "regiao" {
    description = "Regiao AWS do Learner Lab."
    type        = string
    default     = "us-east-1"
  }

  variable "bucket_raw_nome" {
    description = "Nome do bucket S3 raw."
    type        = string
  }

  variable "labrole_arn" {
    description = "ARN da LabRole."
    type        = string
  }

  variable "bucket_gold_nome" {
    description = "Nome do bucket S3 gold."
    type        = string
  }

  variable "dynamodb_tabela" {
    description = "Nome da tabela DynamoDB."
    type        = string
    default     = "execucoes"
  }

  variable "tags" {
    description = "Tags de custo."
    type        = map(string)
    default = {
      Projeto    = "prova-bigdata-aws"
      Disciplina = "Big Data"
      Ambiente   = "LearnerLab"
    }
  }
