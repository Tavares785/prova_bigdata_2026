# Entrega — RA 6325197

## Implementação
Infraestrutura em Terraform, na região us-east-1:
- Buckets raw e gold privados, criados via AWS CLI.
- Glue Job PySpark 4.0, com LabRole e dois workers G.1X.
- Glue Data Catalog e Crawler para as tabelas Parquet.
- Athena Workgroup com resultados no S3.
- DynamoDB sob demanda para metadados das execuções.
- Tags Projeto, Disciplina e Ambiente.

O script descarta pedidos com identificadores ausentes ou quantidade
inválida, substitui textos ausentes nas dimensões por DESCONHECIDO
e grava a fato em Parquet particionado por data_pedido.

## Resultados verificados
- 110 linhas lidas.
- 103 linhas na fato, 21 clientes e 8 produtos.
- 24 partições no formato data_pedido=YYYY-MM-DD.
- Total de vendas: R$ 61.798,90.
- Nenhum produto órfão.
- Glue Job e Crawler concluídos com SUCCEEDED.
- Metadados registrados no DynamoDB com status SUCESSO.
- Os quatro bloqueios públicos foram verificados nos dois buckets.
- Terraform destroy concluído: 10 recursos destruídos.

Durante a execução, a inferência do CSV no Glue gerou datas com horário.
A coluna foi convertida explicitamente para date; o Job foi executado
novamente e o cadastro da fato foi recriado pelo Crawler.

## Arquivos
- infra/: código Terraform e exemplo de variáveis.
- glue-job/: script PySpark.
- dataset/: dataset utilizado.
- athena/: quatro consultas de referência e validação dos totais.
- evidencias/: logs e resultados reais das execuções.

## Reprodução
Configure credenciais temporárias do Learner Lab e copie
infra/terraform.tfvars.example para infra/terraform.tfvars,
preenchendo os nomes dos buckets e o ARN da LabRole.

Execute terraform init, validate, plan e apply na pasta infra.
Dispare o Glue Job, acompanhe até SUCCEEDED e execute o Crawler.
Execute as consultas da pasta athena no banco criado.
Ao finalizar, execute terraform destroy.

## Uso de IA
Utilizei ChatGPT/Codex para orientação, elaboração de código,
interpretação de erros e preparação da entrega. Os comandos foram
executados no meu ambiente e os resultados estão nas evidências.
