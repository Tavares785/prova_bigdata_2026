resource "aws_dynamodb_table" "tabela_execucoes" {
  name         = var.dynamodb_table_nome
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = var.tags
}

