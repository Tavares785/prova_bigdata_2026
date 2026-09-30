# Evidências reais — 30/09/2026

- Terraform 1.16.2, AWS CLI 2.35.6, provider AWS 5.31.0: fmt, init, validate e plan concluídos. Plano: 11 criações, zero alterações/exclusões. Apply: 11 adicionados; plano posterior: No changes. Trecho real: terraform-apply.txt.
- PySpark 3.5.1 no Docker fornecido: testes/test_entrega.py → 3 passed em 41,71 s. Os testes oficiais importam o gabarito do professor; os testes desta entrega importam o script entregue.
- Glue: glue-run.json; JobRunId jr_1c7ff8212a174e44ca24c615afe0e1a3c472cd44dd460bc4eba085350788b06f, SUCCEEDED, 98 s. S3 gold: 24 partições por data. Raw e gold: os quatro bloqueios públicos ativos e as três tags conferidas.
- DynamoDB: dynamodb-item.json; SUCESSO, 110 linhas lidas, 103 gravadas.
- Athena: resultados e IDs em athena-resultados.json. Q1: 7 categorias, soma 61798,90 e nenhuma nula. Q2: 5 clientes em ordem decrescente. Q3: 24 datas, 103 pedidos e soma 61798,90. Q4: zero órfãos de produto. Verificação extra: zero órfãos de cliente.
- Pendentes: prints reais do apply, Q1–Q4 e item DynamoDB; plano de destruição, destroy, conferência de resíduos, push e PR.

Antes da limpeza, salve em docs/prints/ a saída do terminal com tail -20 /tmp/prova-6325231-apply.log, as telas de resultado Q1–Q4 no Athena (banco prova_6325231, workgroup prova-6325231) e o item da tabela prova-6325231-execucoes. Nenhuma imagem foi simulada.
