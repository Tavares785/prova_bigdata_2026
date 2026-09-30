# Prova Big Data — Andreyh Rodrigues de Souza, RA 6325231

Entrega reproduzível em `infra/` e `glue-job/`, com dataset fornecido em `dataset/`. Especificação, tarefas e evidências ficam em `specs/` e `docs/`.

Antes de executar, configure as credenciais temporárias do Learner Lab localmente, incluindo Session Token, e confira o saldo. Use AWS CLI v2 e `us-east-1`. Copie `infra/terraform.tfvars.example` para `infra/terraform.tfvars`, substitua os nomes dos buckets por nomes globais únicos da sua conta e informe o ARN real da LabRole. Não versione o tfvars.

Em `infra/`, rode `terraform init`, `terraform fmt -check`, `terraform validate`, `terraform plan` e revise o plano antes de `terraform apply`. Dispare o Glue Job com os argumentos padrão do Terraform, acompanhe o JobRunId e só então rode as consultas de consultas.sql no banco e workgroup criados. Guarde os prints do apply, consultas e item DynamoDB antes da limpeza. Execute `terraform plan -destroy`, revise os recursos e faça `terraform destroy`; confira buckets e outros resíduos. O `raw.tf` fornecido remove o bucket por CLI e requer atenção especial a objetos versionados ou preexistentes.

Detalhes, pendências e resultados reais: `specs/tasks.md` e `docs/evidencias.md`.

Resultado verificado nesta execução: 110 linhas raw, 103 fatos, 24 partições e faturamento 61798,90. IDs e pendências estão em docs/evidencias.md.
