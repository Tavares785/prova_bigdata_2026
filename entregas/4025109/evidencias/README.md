# Evidências de execução — RA 4025109

Esta pasta reúne os prints/evidências exigidos como entregáveis da prova
(Terraform, consultas Athena e item no DynamoDB).

Salve cada print nesta pasta com o nome indicado abaixo.

## Provisionamento (Terraform)

- `terraform-apply.png` — saída final do `terraform apply` (Apply complete!).
![terraform-apply](terraform-apply.png)
![terraform_plan](terraform-plan.png)
![terraform_validate](terraform_validate.png)

## Glue Job

- `glue-job-succeeded.png` — status `SUCCEEDED` do Job `normaliza-pedidos`.
![glue-job-succeeded](aws_glue.png)

## Camada gold (Parquet particionado)

- `s3-fato-particionado.png` — `aws s3 ls` do `fato_pedidos/` mostrando as
  partições `data_pedido=YYYY-MM-DD/`.
![s3-fato-particionado.png](<aws_s3_fatopedidos.png>)

## Consultas Athena de referência

- `04-athena-consulta1-faturamento-categoria.png` — Consulta 1.
![consulta 1](athena-consulta1-faturamento-categoria.png)

- `athena-consulta2-top5-clientes.png` — Consulta 2.
![athena-consulta2-top5-clientes](athena-consulta2-top5-clientes.png)

- `athena-consulta3-pedidos-por-dia.png` — Consulta 3.
![athena-consulta3-pedidos-por-dia](athena-consulta3-pedidos-por-dia.png)

- `athena-consulta4-integridade.png` — Consulta 4 (orfaos = 0).
![athena-consulta4-integridade](athena-consulta4-integridade.png)

## Metadados no DynamoDB

- `dynamodb-item-execucao.png` — item da tabela `execucoes` com
  `execution_id`, `data_hora`, `dataset`, `linhas_lidas`, `linhas_gravadas`
  e `status = SUCESSO`.

  ![dynamodb-item-execucao](dynamodb-item-execucao.png)

## Limpeza (ao final)

- `terraform-destroy.png` — saída do `terraform destroy` (Destroy complete!).
![terraform-destroy](terraform-destroy.png)