resource "aws_dynamodb_table" "execucoes" {
  name         = "execucoes-6325197"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"

  attribute {
    name = "execution_id"
    type = "S"
  }

  tags = var.tags
}

output "ddb_table_nome" {
  value = aws_dynamodb_table.execucoes.name
}
