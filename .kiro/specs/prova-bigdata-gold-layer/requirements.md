# Documento de Requisitos — Camada Gold (Prova Prática Big Data AWS)

## Introdução

Esta feature cobre a construção da **camada gold** da prova prática de Big Data na AWS.
O aluno parte de uma infraestrutura raw já entregue pelo professor (Bucket_Raw com o dataset CSV
de pedidos desnormalizado) e deve construir dois artefatos:

1. **`infra/gold.tf`** — Terraform que provisiona Bucket_Gold, Glue Job, Glue Data Catalog,
   Athena Workgroup e tabela DynamoDB de metadados, seguindo as restrições do AWS Academy
   Learner Lab (região `us-east-1`, LabRole por ARN, buckets via AWS CLI).

2. **`glue-job/normaliza_pedidos.py`** — Glue Job PySpark que lê o CSV desnormalizado do
   Bucket_Raw, aplica as regras de limpeza e normaliza em um esquema estrela
   (`fato_pedidos` + `dim_cliente` + `dim_produto`), grava Parquet particionado no Bucket_Gold
   e registra metadados da execução no DynamoDB.

Os arquivos finais devem ser entregues na pasta `entregas/<RA>/` via Pull Request.

---

## Glossário

- **Bucket_Raw**: bucket S3 de origem, criado pelo professor, contém o arquivo
  `pedidos/pedidos_desnormalizado.csv`.
- **Bucket_Gold**: bucket S3 de destino (privado), criado pelo aluno, onde os arquivos
  Parquet normalizados são gravados.
- **Dataset_Exemplo**: arquivo `pedidos_desnormalizado.csv` com 110 linhas e 11 colunas
  (`pedido_id`, `data_pedido`, `cliente_id`, `cliente_nome`, `cliente_uf`, `produto_id`,
  `produto_nome`, `categoria`, `preco_unitario`, `quantidade`, `valor_total`).
- **Modelo_Dimensional_Alvo**: esquema estrela composto por `fato_pedidos`, `dim_cliente`
  e `dim_produto`.
- **fato_pedidos**: tabela fato com colunas `pedido_id` (PK), `data_pedido` (partição),
  `cliente_id` (FK), `produto_id` (FK), `preco_unitario`, `quantidade`, `valor_total`.
- **dim_cliente**: tabela dimensão com `cliente_id` (PK), `cliente_nome`, `cliente_uf`.
- **dim_produto**: tabela dimensão com `produto_id` (PK), `produto_nome`, `categoria`.
- **Job_Normalizacao**: Glue Job PySpark que executa a normalização (`normaliza_pedidos.py`).
- **Glue_Data_Catalog**: banco de metadados do AWS Glue com o database e as três tabelas
  do Modelo_Dimensional_Alvo.
- **Athena_Workgroup**: workgroup do Amazon Athena que executa consultas sobre o
  Glue_Data_Catalog e grava resultados no Bucket_Gold.
- **DynamoDB_Execucoes**: tabela DynamoDB que registra os metadados de cada execução do
  Job_Normalizacao (chave de partição: `execution_id`).
- **LabRole**: IAM Role fornecida pelo AWS Academy Learner Lab, referenciada por ARN,
  usada por todos os serviços que precisam de permissões (Glue Job, etc.).
- **Terraform_Infra**: conjunto de arquivos `.tf` na pasta `infra/` que provisionam a
  infraestrutura via `terraform apply`.
- **Infraestrutura_Gold**: recursos AWS provisionados pelo aluno em `infra/gold.tf`.
- **Linha_Invalida**: linha do Dataset_Exemplo com `pedido_id`, `cliente_id` ou `produto_id`
  nulo/vazio, ou com `quantidade` nula/`<= 0`.
- **DESCONHECIDO**: valor sentinela `"DESCONHECIDO"` usado para preencher textos ausentes
  nas dimensões.
- **Parquet_Particionado**: formato de arquivo Parquet gravado com particionamento por
  `data_pedido` no layout `<tabela>/data_pedido=YYYY-MM-DD/part-*.parquet`.
- **Tags_Obrigatorias**: conjunto de tags AWS obrigatórias: `Projeto`, `Disciplina`,
  `Ambiente`.

---

## Requisitos

### Requisito 1 — Infraestrutura: Bucket_Gold privado

**User Story:** Como aluno, quero provisionar o Bucket_Gold via Terraform, para que os dados
normalizados tenham um destino S3 privado e gerenciado como código.

#### Critérios de Aceitação

1. WHEN o aluno executa `terraform apply` na pasta `infra/`, THE Terraform_Infra SHALL criar o
   Bucket_Gold usando `terraform_data` com `local-exec` e AWS CLI (não com o recurso
   `aws_s3_bucket`), seguindo o mesmo padrão do `raw.tf`.
2. WHEN o Bucket_Gold é criado, THE Terraform_Infra SHALL habilitar os quatro bloqueios de
   acesso público (`BlockPublicAcls`, `IgnorePublicAcls`, `BlockPublicPolicy`,
   `RestrictPublicBuckets`) via `aws s3api put-public-access-block`.
3. THE Terraform_Infra SHALL declarar a variável `bucket_gold_nome` em `variables.tf` com
   validação de comprimento (3 a 63 caracteres), sem valor padrão.
4. WHEN o aluno executa `terraform destroy`, THE Terraform_Infra SHALL esvaziar e remover o
   Bucket_Gold via provisioner `local-exec when = destroy` usando `aws s3 rb --force`.
5. THE Terraform_Infra SHALL aplicar as Tags_Obrigatorias (`Projeto`, `Disciplina`, `Ambiente`)
   ao recurso `terraform_data` do Bucket_Gold (ou equivalente) de forma consistente com todos
   os demais recursos.

---

### Requisito 2 — Infraestrutura: Glue Job

**User Story:** Como aluno, quero provisionar o Glue Job via Terraform usando a LabRole, para que
o Job_Normalizacao possa ser executado na AWS sem criar roles IAM próprias.

#### Critérios de Aceitação

1. THE Terraform_Infra SHALL declarar a variável `labrole_arn` em `variables.tf` como string
   obrigatória (sem valor padrão) para receber o ARN da LabRole.
2. WHEN o aluno executa `terraform apply`, THE Terraform_Infra SHALL criar o recurso
   `aws_glue_job` referenciando o script `normaliza_pedidos.py` no Bucket_Gold (ou Bucket_Raw)
   e usando `labrole_arn` no campo `role_arn`.
3. THE Glue_Job SHALL ser configurado com os argumentos padrão `--RAW_PATH`, `--GOLD_PATH`,
   `--DDB_TABLE` e `--DATASET_NAME` apontando para os recursos provisionados.
4. THE Terraform_Infra SHALL aplicar as Tags_Obrigatorias ao recurso `aws_glue_job`.
5. IF o aluno tentar criar um recurso `aws_iam_role` ou `aws_iam_policy` em qualquer arquivo
   `.tf`, THEN THE Terraform_Infra SHALL falhar na validação ou o aluno SHALL remover esses
   recursos antes do `apply` (restrição: usar apenas a LabRole existente).

---

### Requisito 3 — Infraestrutura: Glue Data Catalog

**User Story:** Como aluno, quero registrar as três tabelas do Modelo_Dimensional_Alvo no Glue
Data Catalog, para que o Athena consiga consultá-las sem configuração adicional.

#### Critérios de Aceitação

1. WHEN o aluno executa `terraform apply`, THE Terraform_Infra SHALL criar um
   `aws_glue_catalog_database` com um nome único (ex.: `bigdata_gold_<RA>`).
2. THE Terraform_Infra SHALL criar três recursos `aws_glue_catalog_table` (um para cada tabela:
   `fato_pedidos`, `dim_cliente`, `dim_produto`) apontando para os prefixos correspondentes no
   Bucket_Gold.
3. THE Glue_Data_Catalog SHALL registrar `fato_pedidos` com `storage_descriptor` no formato
   `org.apache.hadoop.hive.ql.io.parquet` e com coluna de partição `data_pedido` (tipo `date`).
4. THE Glue_Data_Catalog SHALL registrar `dim_cliente` com as colunas `cliente_id` (string),
   `cliente_nome` (string) e `cliente_uf` (string).
5. THE Glue_Data_Catalog SHALL registrar `dim_produto` com as colunas `produto_id` (string),
   `produto_nome` (string) e `categoria` (string).
6. THE Terraform_Infra SHALL aplicar as Tags_Obrigatorias ao `aws_glue_catalog_database`.

---

### Requisito 4 — Infraestrutura: Athena Workgroup

**User Story:** Como aluno, quero provisionar um Athena Workgroup via Terraform, para que as
consultas analíticas sobre o Modelo_Dimensional_Alvo tenham um destino de resultados configurado.

#### Critérios de Aceitação

1. WHEN o aluno executa `terraform apply`, THE Terraform_Infra SHALL criar um
   `aws_athena_workgroup` com resultado de consultas configurado para um prefixo dentro do
   Bucket_Gold (ex.: `s3://<gold>/athena-results/`).
2. THE Athena_Workgroup SHALL ter `enforce_workgroup_configuration = true` para que o output
   location seja obrigatório.
3. THE Terraform_Infra SHALL aplicar as Tags_Obrigatorias ao `aws_athena_workgroup`.

---

### Requisito 5 — Infraestrutura: Tabela DynamoDB de Metadados

**User Story:** Como aluno, quero provisionar a DynamoDB_Execucoes via Terraform, para que o
Job_Normalizacao possa registrar os metadados de cada execução com rastreabilidade.

#### Critérios de Aceitação

1. WHEN o aluno executa `terraform apply`, THE Terraform_Infra SHALL criar um recurso
   `aws_dynamodb_table` com o nome configurável via variável (ex.: `dynamodb_table_nome`).
2. THE DynamoDB_Execucoes SHALL ter `execution_id` (tipo String) como chave de partição
   (`hash_key`), sem chave de ordenação.
3. THE DynamoDB_Execucoes SHALL usar `billing_mode = "PAY_PER_REQUEST"` para evitar custo fixo
   de capacidade provisionada.
4. THE Terraform_Infra SHALL aplicar as Tags_Obrigatorias ao recurso `aws_dynamodb_table`.

---

### Requisito 6 — Job PySpark: Normalização para Modelo Dimensional

**User Story:** Como aluno, quero implementar a função `normalizar(df_raw)` no
`normaliza_pedidos.py`, para que o Job_Normalizacao produza o Modelo_Dimensional_Alvo correto
a partir do Dataset_Exemplo.

#### Critérios de Aceitação

1. THE Job_Normalizacao SHALL produzir exatamente três DataFrames com as chaves `"fato_pedidos"`,
   `"dim_cliente"` e `"dim_produto"` como retorno da função `normalizar`.
2. THE Job_Normalizacao SHALL garantir que `dim_cliente` contenha exatamente uma linha por
   `cliente_id` distinto (sem duplicatas).
3. THE Job_Normalizacao SHALL garantir que `dim_produto` contenha exatamente uma linha por
   `produto_id` distinto (sem duplicatas).
4. THE Job_Normalizacao SHALL garantir que `fato_pedidos` contenha exatamente uma linha por
   `pedido_id` distinto proveniente de linhas válidas do Dataset_Exemplo.
5. WHEN uma linha do Dataset_Exemplo possui `pedido_id`, `cliente_id` ou `produto_id` nulo ou
   vazio (após trim), THEN THE Job_Normalizacao SHALL descartar essa linha do `fato_pedidos`.
6. WHEN uma linha do Dataset_Exemplo possui `quantidade` nula ou `<= 0`, THEN THE
   Job_Normalizacao SHALL descartar essa linha do `fato_pedidos`.
7. WHEN `cliente_nome` ou `cliente_uf` está ausente ou vazio em uma linha com `cliente_id`
   válido, THEN THE Job_Normalizacao SHALL preencher o campo com o valor `"DESCONHECIDO"` na
   `dim_cliente`.
8. WHEN `produto_nome` ou `categoria` está ausente ou vazio em uma linha com `produto_id`
   válido, THEN THE Job_Normalizacao SHALL preencher o campo com o valor `"DESCONHECIDO"` na
   `dim_produto`.
9. THE Job_Normalizacao SHALL garantir que todo `cliente_id` presente em `fato_pedidos` também
   exista em `dim_cliente` (integridade referencial).
10. THE Job_Normalizacao SHALL garantir que todo `produto_id` presente em `fato_pedidos` também
    exista em `dim_produto` (integridade referencial).
11. THE Job_Normalizacao SHALL operar apenas sobre DataFrames e valores Python (sem chamadas a
    serviços AWS) dentro da função `normalizar`, permitindo execução em SparkSession local.

---

### Requisito 7 — Job PySpark: Gravação em Parquet Particionado

**User Story:** Como aluno, quero implementar a função `escrever_gold(tabelas, gold_path)`, para
que os dados normalizados sejam persistidos no Bucket_Gold em formato Parquet consultável pelo
Athena.

#### Critérios de Aceitação

1. WHEN a função `escrever_gold` é chamada, THE Job_Normalizacao SHALL gravar `fato_pedidos`
   em Parquet no prefixo `<gold_path>/fato_pedidos/` particionado pela coluna `data_pedido`.
2. THE Job_Normalizacao SHALL produzir uma partição física distinta por valor de `data_pedido`
   no layout `fato_pedidos/data_pedido=YYYY-MM-DD/part-*.parquet`.
3. THE Job_Normalizacao SHALL gravar `dim_cliente` em Parquet no prefixo
   `<gold_path>/dim_cliente/` sem particionamento.
4. THE Job_Normalizacao SHALL gravar `dim_produto` em Parquet no prefixo
   `<gold_path>/dim_produto/` sem particionamento.
5. WHEN o conteúdo de `fato_pedidos` é gravado em Parquet e relido pelo Spark com o mesmo
   schema, THE Job_Normalizacao SHALL produzir um conjunto de linhas idêntico ao original
   (propriedade round-trip).
6. WHEN `fato_pedidos` possui N datas distintas em `data_pedido`, THEN THE Job_Normalizacao
   SHALL criar exatamente N diretórios de partição `data_pedido=...` no Bucket_Gold.

---

### Requisito 8 — Job PySpark: Metadados de Execução no DynamoDB

**User Story:** Como aluno, quero implementar `montar_metadados` e `gravar_metadados_dynamo`,
para que cada execução do Job_Normalizacao seja rastreável via DynamoDB.

#### Critérios de Aceitação

1. THE Job_Normalizacao SHALL retornar da função `montar_metadados` um dict com as chaves
   `execution_id`, `data_hora`, `dataset`, `linhas_lidas`, `linhas_gravadas` e `status`.
2. THE Job_Normalizacao SHALL preencher `data_hora` com o instante da execução em formato
   ISO-8601 UTC (ex.: `"2026-02-01T14:30:00Z"`).
3. THE Job_Normalizacao SHALL preencher `linhas_lidas` com a contagem de linhas do DataFrame
   lido do Bucket_Raw antes de qualquer filtragem.
4. THE Job_Normalizacao SHALL preencher `linhas_gravadas` com a contagem de linhas do
   `fato_pedidos` após a normalização e descarte de Linha_Invalida.
5. WHEN o Job_Normalizacao conclui com sucesso, THE Job_Normalizacao SHALL gravar o item de
   metadados na DynamoDB_Execucoes com `status = "SUCESSO"` usando `boto3.put_item`.
6. IF o Job_Normalizacao falha com uma exceção, THEN THE Job_Normalizacao SHALL gravar o item
   de metadados na DynamoDB_Execucoes com `status = "FALHA"`, `linhas_lidas = 0` e
   `linhas_gravadas = 0`, antes de relançar a exceção.
7. THE função `montar_metadados` SHALL operar apenas sobre valores Python (sem chamadas a
   serviços AWS), permitindo execução e teste local sem credenciais AWS.

---

### Requisito 9 — Job PySpark: Leitura do CSV Raw

**User Story:** Como aluno, quero implementar a função `ler_raw(spark, raw_path)`, para que o
Job_Normalizacao carregue o Dataset_Exemplo do Bucket_Raw como DataFrame Spark.

#### Critérios de Aceitação

1. WHEN a função `ler_raw` é chamada com um `raw_path` válido, THE Job_Normalizacao SHALL ler o
   arquivo CSV com cabeçalho (`header=True`) e inferência de schema (`inferSchema=True` ou
   schema explícito compatível com as 11 colunas do Dataset_Exemplo).
2. THE Job_Normalizacao SHALL retornar um DataFrame com as colunas `pedido_id`, `data_pedido`,
   `cliente_id`, `cliente_nome`, `cliente_uf`, `produto_id`, `produto_nome`, `categoria`,
   `preco_unitario`, `quantidade` e `valor_total`.
3. THE Job_Normalizacao SHALL contar as linhas do DataFrame retornado por `ler_raw` antes de
   qualquer filtro para registrar `linhas_lidas` nos metadados (Requisito 8.3).

---

### Requisito 10 — Consultas Athena

**User Story:** Como aluno, quero executar pelo menos 3 consultas de referência no Athena sobre
o Modelo_Dimensional_Alvo, para evidenciar que os dados foram gravados e catalogados corretamente.

#### Critérios de Aceitação

1. WHEN as tabelas `fato_pedidos`, `dim_cliente` e `dim_produto` estão registradas no
   Glue_Data_Catalog, THE Athena_Workgroup SHALL executar `SELECT COUNT(*) FROM fato_pedidos`
   retornando um valor maior que zero.
2. THE Athena_Workgroup SHALL executar um JOIN entre `fato_pedidos` e `dim_cliente` retornando
   pelo menos as colunas `pedido_id`, `cliente_nome` e `valor_total`.
3. THE Athena_Workgroup SHALL executar uma consulta de agregação por `data_pedido` em
   `fato_pedidos` retornando o total de pedidos por data.
4. THE Athena_Workgroup SHALL usar o output location configurado no Bucket_Gold para salvar os
   resultados das consultas.

---

### Requisito 11 — Segurança, Custo e Limpeza

**User Story:** Como aluno, quero seguir as boas práticas de segurança e custo do Learner Lab,
para que a infraestrutura não deixe recursos ociosos nem exponha dados publicamente.

#### Critérios de Aceitação

1. THE Terraform_Infra SHALL usar `var.regiao = "us-east-1"` em todos os recursos, sem
   hardcode da região em strings literais.
2. THE Terraform_Infra SHALL garantir que o Bucket_Gold seja privado, com os quatro bloqueios
   de acesso público habilitados (Requisito 1.2).
3. THE Terraform_Infra SHALL referenciar a LabRole exclusivamente por ARN via variável
   `labrole_arn`, sem criar recursos `aws_iam_role` ou `aws_iam_policy`.
4. THE Terraform_Infra SHALL garantir que o arquivo `terraform.tfvars` não seja versionado
   (listado no `.gitignore`), protegendo valores sensíveis como `labrole_arn`.
5. THE Terraform_Infra SHALL aplicar as Tags_Obrigatorias (`Projeto`, `Disciplina`, `Ambiente`)
   a todos os recursos criados em `gold.tf`.
6. WHEN o aluno executa `terraform destroy` ao final da prova, THE Terraform_Infra SHALL
   remover todos os recursos criados, incluindo o esvaziamento e exclusão do Bucket_Gold.

---

### Requisito 12 — Entrega via Pull Request

**User Story:** Como aluno, quero entregar os artefatos no padrão definido pelo professor, para
que a correção seja objetiva e reproduzível.

#### Critérios de Aceitação

1. THE aluno SHALL criar um fork do repositório original e abrir uma Pull Request a partir de
   uma branch no padrão `prova-<RA>` para a `master` do repositório original.
2. THE aluno SHALL colocar os arquivos de entrega em `entregas/<RA>/` (subpastas `infra/` e
   `glue-job/`), não alterando os arquivos fora dessa pasta.
3. THE aluno SHALL incluir na Pull Request evidências de execução: print do `terraform apply`
   bem-sucedido e prints das consultas Athena com resultados visíveis.
4. THE aluno SHALL incluir evidência do `terraform destroy` concluído ao final.
5. WHERE o aluno utilizar credenciais temporárias do Learner Lab, THE aluno SHALL configurá-las
   exclusivamente via variáveis de ambiente ou `~/.aws/credentials`, nunca em arquivos
   versionados.

---

### Requisito 13 — Critérios de Avaliação (Rubrica)

**User Story:** Como professor, quero uma rubrica objetiva, para que a correção das entregas
seja justa e reproduzível com base em evidências verificáveis.

#### Critérios de Aceitação

1. WHEN o professor executa `terraform init`, `terraform validate` e `terraform apply` na pasta
   de entrega do aluno, THE Terraform_Infra SHALL concluir sem erros e criar Bucket_Gold, Glue
   Job, Glue_Data_Catalog, Athena_Workgroup e DynamoDB_Execucoes. *(20 pontos)*
2. WHEN o professor executa o Job_Normalizacao com o Dataset_Exemplo, THE Job_Normalizacao
   SHALL produzir `fato_pedidos`, `dim_cliente` e `dim_produto` com chaves únicas, integridade
   referencial, descarte correto de Linha_Invalida e substituição por DESCONHECIDO. *(25 pontos)*
3. WHEN o professor inspeciona o Bucket_Gold após a execução do Job_Normalizacao, THE
   Bucket_Gold SHALL conter os dados em Parquet_Particionado com uma partição por data distinta
   de `data_pedido`. *(15 pontos)*
4. WHEN o professor executa as consultas de referência no Athena_Workgroup, THE
   Athena_Workgroup SHALL retornar os resultados esperados conforme o Requisito 10. *(15 pontos)*
5. WHEN o professor inspeciona a DynamoDB_Execucoes após a execução do Job_Normalizacao, THE
   DynamoDB_Execucoes SHALL conter um item com os campos `execution_id`, `data_hora`, `dataset`,
   `linhas_lidas`, `linhas_gravadas` e `status` preenchidos e coerentes. *(10 pontos)*
6. WHEN o professor revisa o repositório e a infraestrutura, THE Terraform_Infra SHALL
   apresentar Tags_Obrigatorias em todos os recursos, Bucket_Gold privado, uso exclusivo da
   LabRole, credenciais não versionadas e evidência de `terraform destroy`. *(15 pontos)*
7. THE pontuação total dos Critérios 13.1 a 13.6 SHALL somar 100 pontos, podendo ser
   reescalada para outras bases pelo professor preservando os pesos relativos.
