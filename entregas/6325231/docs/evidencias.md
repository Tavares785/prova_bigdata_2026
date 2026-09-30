# Evidências reais — 30/09/2026

- Terraform 1.16.2, AWS CLI 2.35.6, provider AWS 5.31.0: fmt, init, validate e plan concluídos. Plano: 11 criações, zero alterações/exclusões. Apply: 11 adicionados; plano posterior: No changes. Trecho real: terraform-apply.txt.
- PySpark 3.5.1 no Docker fornecido: testes/test_entrega.py → 3 passed em 41,71 s. Os testes oficiais importam o gabarito do professor; os testes desta entrega importam o script entregue.
- Glue: glue-run.json; JobRunId jr_1c7ff8212a174e44ca24c615afe0e1a3c472cd44dd460bc4eba085350788b06f, SUCCEEDED, 98 s. S3 gold: 24 partições por data. Raw e gold: os quatro bloqueios públicos ativos e as três tags conferidas.
- DynamoDB: dynamodb-item.json; SUCESSO, 110 linhas lidas, 103 gravadas.
- Athena: resultados e IDs em athena-resultados.json. Q1: 7 categorias, soma 61798,90 e nenhuma nula. Q2: 5 clientes em ordem decrescente. Q3: 24 datas, 103 pedidos e soma 61798,90. Q4: zero órfãos de produto. Verificação extra: zero órfãos de cliente.
- Prints reais: apply da primeira conta em `prints/01-terraform-primeira-conta.png`; Q1–Q6 e item DynamoDB da segunda conta em `prints/02` a `08`. A segunda conta tem também log real de apply em `execucao-906975261211/terraform-apply.txt`.

Nenhuma imagem foi simulada. O print de apply é da primeira conta; os prints de consultas e DynamoDB são da segunda, como indicado pelos IDs de conta.

## Nova sessão Learner Lab — conta 906975261211

A sessão renovada usa uma conta AWS diferente da primeira (265062041401). Em workspace Terraform isolado lab-906975261211, o plano teve 11 criações; apply concluiu e o plano posterior mostrou No changes. Glue JobRunId jr_2a7f17583e36c003ca16d8825ed1033e36ed0d600aee53f35a491922a55e02c6: SUCCEEDED. DynamoDB: 110 linhas lidas, 103 gravadas, SUCESSO. Athena: Q1 efaf47f3-e57f-4a9c-9cfb-213cdaa33850; Q2 195187f3-f08f-4868-b60c-9fad1fce2ae2; Q3 7dd6c7b9-7dbc-4bcf-852e-b4ea03ced4d3; Q4 e40fe590-aa5d-4d56-9271-b247b2a14da0. As consultas retornaram 7 categorias, 5 clientes, 24 datas, 103 fatos, faturamento 61798,90 e zero órfãos. Arquivos reais separados em docs/execucao-906975261211/.

Após os prints, o destroy na conta 906975261211 concluiu. O Athena exigiu exclusão recursiva do workgroup com histórico de consultas; a repetição removeu o último recurso. Evidência em `execucao-906975261211/terraform-destroy.txt`. Estado Terraform vazio e buckets, DynamoDB, Glue e workgroup ausentes. A sessão temporária da primeira conta terminou e não há mais acesso para verificar sua limpeza. Seu estado Terraform local permanece preservado no workspace default, sem prova de recursos ativos ou removidos nessa conta. A limpeza da conta atual foi verificada. Push e PR ficam para a entrega.

Auditoria de 30/09: o primeiro destroy da conta atual falhou porque o workgroup Athena tinha histórico. `force_destroy = true` foi acrescentado em ambas as cópias de `gold.tf` para futuras execuções; `terraform validate` passou. A execução real registrada acima usou exclusão recursiva via AWS CLI e repetição do plano de destroy.
