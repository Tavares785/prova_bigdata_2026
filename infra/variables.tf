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

# =============================================================================
# Variáveis da camada GOLD (construídas pelo aluno)
# =============================================================================

# Nome global único do Bucket_Gold (camada gold — Parquet normalizado).
# Sem "default" de propósito: o nome muda de aluno para aluno (leva o RA),
# então a variável é OBRIGATÓRIA — se faltar no terraform.tfvars, o Terraform
# para e pede o valor, em vez de usar um nome genérico que pode colidir.
variable "bucket_gold_nome" {
  description = "Nome global único do bucket S3 gold (Parquet particionado, saída do Glue Job)."
  type        = string

  # Regra de nome de bucket da AWS: 3 a 63 caracteres, só minúsculas, números,
  # "." e "-", começando e terminando com letra/número.
  # 🔒 Também é uma proteção de SEGURANÇA: este valor é interpolado dentro de
  # comandos de shell (local-exec). Aceitando só esses caracteres, fica
  # impossível "injetar" algo como  meu-bucket"; rm -rf ~; echo "  no comando.
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.bucket_gold_nome))
    error_message = "Nome de bucket inválido: 3-63 caracteres, apenas a-z, 0-9, '.' e '-', começando e terminando com letra ou número."
  }
}

# ARN da LabRole pré-provisionada do Learner Lab. No Learner Lab NÃO podemos
# criar roles/policies, então o Glue Job apenas REFERENCIA esta role existente.
# Formato: arn:aws:iam::<ID_DA_CONTA_12_DIGITOS>:role/LabRole
# Sem "default": o ID da conta é diferente para cada aluno.
variable "labrole_arn" {
  description = "ARN da role LabRole do Learner Lab (usada como IAM role do Glue Job)."
  type        = string

  validation {
    # regex() compara o valor com uma expressão regular:
    #   ^                  -> início do texto
    #   arn:aws:iam::      -> prefixo fixo de todo ARN de IAM
    #   [0-9]{12}          -> exatamente 12 dígitos (ID da conta AWS)
    #   :role/LabRole      -> o nome da role
    #   $                  -> fim do texto (nada pode vir depois)
    # Quando NÃO casa, regex() gera um ERRO (não retorna false). Por isso
    # envolvemos em can(), que converte "deu erro" em false e "funcionou" em true.
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/LabRole$", var.labrole_arn))
    error_message = "labrole_arn deve ter o formato arn:aws:iam::<12 dígitos>:role/LabRole."
  }
}

# Tags de custo padronizadas, aplicadas em todos os recursos que suportam tags
# (tags = var.tags). Servem para identificar e somar os custos do projeto no
# billing da AWS (ex.: "quanto gastou tudo com Projeto = prova-bigdata?").
#
# map(string) = coleção de pares chave -> valor, onde TODOS os valores são texto.
# Tem "default" porque esses valores não são sensíveis nem mudam por aluno;
# ainda assim podem ser sobrescritos no terraform.tfvars.
variable "tags" {
  description = "Tags de custo padronizadas (Projeto, Disciplina, Ambiente) aplicadas aos recursos."
  type        = map(string)

  default = {
    Projeto    = "prova-bigdata"
    Disciplina = "BigData"
    Ambiente   = "prova"
  }

  validation {
    # Garante que as 3 chaves exigidas pelo README (seção 11) estão presentes.
    #   keys(var.tags)              -> lista das chaves do map, ex.: ["Ambiente", "Disciplina", "Projeto"]
    #   contains(lista, k)          -> true se k está na lista
    #   [for k in [...] : ...]      -> "for expression": gera uma lista de true/false, uma por chave exigida
    #   alltrue(lista)              -> true só se TODOS os itens forem true
    condition = alltrue([
      for k in ["Projeto", "Disciplina", "Ambiente"] : contains(keys(var.tags), k)
    ])
    error_message = "tags deve conter as chaves Projeto, Disciplina e Ambiente."
  }
}

# Nome da tabela DynamoDB que guarda os metadados de cada execução do Glue Job.
# Tem "default" porque o nome de tabela DynamoDB só precisa ser único DENTRO da
# conta/região (diferente de bucket S3, que é global). "execucoes" é o nome usado
# no exemplo do README (--DDB_TABLE).
variable "ddb_tabela_nome" {
  description = "Nome da tabela DynamoDB de metadados das execuções do Glue Job."
  type        = string
  default     = "execucoes"
}

# Nome do database no Glue Data Catalog (onde ficam as tabelas que o Athena lê).
# Use apenas minúsculas, números e "_": o Athena não aceita hífen em nomes de
# database/tabela sem aspas especiais (`assim`), o que complica as consultas.
variable "glue_database_nome" {
  description = "Nome do database do Glue Data Catalog para as tabelas do gold."
  type        = string
  default     = "prova_bigdata"

  validation {
    condition     = can(regex("^[a-z0-9_]+$", var.glue_database_nome))
    error_message = "Use apenas letras minúsculas, números e underscore (sem hífen)."
  }
}
