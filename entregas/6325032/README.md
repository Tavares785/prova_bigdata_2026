# Prova Prática de Big Data — RA 6325032

**Daniel de Oliveira Tavares Junior · Big Data — UNIFAAT**

Implementação: S3 raw → Glue/PySpark → Parquet no S3 gold → Glue Catalog/Athena,
com metadados de execução no DynamoDB.

## O que está incluído

- `infra/raw.tf`: lógica enviada pelo professor, transcrita sem os escapes de Markdown/HTML.
- `infra/gold.tf`: bucket privado via AWS CLI, script no S3, Glue Job, catálogo,
  workgroup Athena, DynamoDB e tags dos buckets.
- `infra/variables.tf` e `versions.tf`: declarações que não estavam no trecho raw enviado.
- `glue-job/normaliza_pedidos.py`: implementação das funções do esqueleto.
- `dataset/`: cópia exata do CSV recebido.
- `tests/`: testes da implementação do aluno, independentes do gabarito.
- `sql/`: registro de partições, quatro consultas da prova e validações adicionais.
- `RESULTADOS_ESPERADOS.md`: valores calculados localmente a partir do CSV.
- `evidencias/`: resultados locais e lista das evidências AWS ainda necessárias.

Não foi recebido o repositório nem o conteúdo executável de `local-test/`.
Os testes adicionais ficam em `tests/`; os arquivos originais de `local-test/`
devem ser preservados quando a entrega for integrada ao fork.
Se o repositório original já declarar providers/variáveis/versões, reaproveite
essas declarações sem duplicá-las. Compare o `raw.tf` original com a transcrição.

## Decisões de normalização

1. Chaves nulas, vazias ou só com espaços, e quantidades ausentes ou não positivas,
   eliminam a linha antes de construir o modelo dimensional.
2. As dimensões representam os clientes e produtos dos itens válidos. Clientes
   presentes apenas em linhas rejeitadas não geram dimensões sem vendas.
3. Para uma chave repetida, escolhe-se a linha com mais atributos textuais presentes.
   Empates usam ordem lexicográfica. Isso evita uma escolha aleatória entre, por
   exemplo, `Smartphone X/Eletronicos` e o mesmo produto sem categoria.
4. Atributos ausentes na linha escolhida recebem `DESCONHECIDO`. Uma dimensão cujo
   único cadastro não tem textos continua existindo com esse valor.
5. `pedido_id` é único no fato. Duplicatas usam desempate lexicográfico pelos campos
   do fato; o dataset recebido não contém duplicatas válidas desse identificador.
6. Preço e valor total mantêm o tipo `double` do enunciado. `valor_total` é preservado
   da origem; o arredondamento ocorre nas consultas, sem alterar as medidas.
7. O fato é particionado por data; as dimensões não têm partições. A gravação usa
   Parquet/Snappy e substitui o snapshot anterior, removendo partições antigas.
8. Uma data ausente/inválida num item válido causa FALHA no job, em vez de produzir
   silenciosamente uma partição Hive nula. Essa condição não ocorre no CSV recebido.

A escrita das três tabelas não é transacional. Aguarde `SUCCEEDED` antes de consultar.
Em falha, o contador de gravação só confirma o fato depois da escrita completa;
arquivos parciais podem existir. O job registra `FALHA` quando o DynamoDB está
acessível e mantém a exceção original se o registro de metadados também falhar.

## Teste local

Na pasta desta entrega, com Docker ativo:

```bash
docker build -f tests/Dockerfile -t prova-6325032-local .
docker run --rm prova-6325032-local
```

Este ambiente adicional usa Python 3.11, Java 17 e PySpark 3.5.1. Java 17 foi escolhido
para aproximar o runtime do Glue 5.0; o ambiente original do professor com Java 21
não foi recebido nem modificado. O teste lê o CSV real, executa a implementação,
confere integridade, testa o Parquet no disco, reexecução e 100 exemplos gerados.
O Dockerfile instala ferramentas só no container.

## Preparar o Learner Lab

1. Inicie uma sessão do Learner Lab e configure as credenciais temporárias localmente,
   incluindo o session token. Não coloque credenciais no código nem no Git.
2. Use um terminal com AWS CLI v2 e Terraform 1.4 ou superior.
3. Configure a região e confira a identidade antes de criar recursos:

```bash
export AWS_DEFAULT_REGION=us-east-1
export AWS_REGION=us-east-1
export AWS_PAGER=""
aws --version
aws sts get-caller-identity --region us-east-1
```

4. Copie `infra/terraform.tfvars.example` para `infra/terraform.tfvars`.
   Substitua `SUA-CONTA` pelo ID da sua conta e confira o ARN real da LabRole.
   Os dois nomes de bucket devem ser novos e exclusivos desta atividade.
   O ID da conta pode ser usado como sufixo; nomes S3 são globalmente únicos.

O `raw.tf` pronto contém `|| true` na criação e destruição do bucket. Por isso,
use um nome raw novo e confira a remoção ao final: o retorno do Terraform sozinho
não comprova que esse bucket foi apagado. A lógica fornecida foi preservada.
No gold, uma tag de identificação evita remover um bucket que não pertença ao
recurso Terraform. Se a criação falhar antes de salvar essa tag, confira e limpe
o recurso parcial no laboratório antes de tentar de novo.

## Aplicar a infraestrutura

Na pasta desta entrega:

```bash
cd infra
terraform init
terraform fmt -check
terraform validate
terraform plan -out=plano.tfplan
terraform apply plano.tfplan
terraform output
```

Guarde o print do apply completo. O apply cria raw e gold na mesma operação;
ele não inicia o job automaticamente. O provider usa `us-east-1`, sem `default_tags`.
Os recursos compatíveis recebem tags explícitas; as tabelas do catálogo não têm
campo de tags neste recurso do provider. O database e o job têm tags.

## Executar o Glue

Ainda em `infra/`:

```bash
JOB_NOME=$(terraform output -raw glue_job_nome)
RUN_ID=$(aws glue start-job-run --job-name "$JOB_NOME" --region us-east-1 \
  --query JobRunId --output text)
echo "$RUN_ID"
aws glue get-job-run --job-name "$JOB_NOME" --run-id "$RUN_ID" \
  --region us-east-1 --query 'JobRun.{Status:JobRunState,Erro:ErrorMessage}'
```

Repita a consulta de status ou acompanhe no console até `SUCCEEDED`.
Os quatro argumentos do job já estão configurados pelo Terraform.
O job usa Glue 5.0, dois workers G.1X, timeout de 10 minutos, zero retries automáticos
e uma execução por vez. Falhas de permissão ou restrições específicas da conta do
Learner Lab só podem ser confirmadas na execução real.

## Consultar e verificar

No Athena, selecione o workgroup `prova-bigdata-6325032`, fonte `AwsDataCatalog`
e database `prova_bigdata_6325032`.

1. Execute `sql/00_registrar_particoes.sql` e aguarde sucesso. Sem esse passo,
   a tabela fato pode parecer vazia mesmo com os Parquets no S3.
2. Execute separadamente as quatro consultas `01` a `04` e compare com
   `RESULTADOS_ESPERADOS.md`.
3. Guarde prints mostrando SQL, resultado, database e workgroup.
4. Confira no S3 as 24 pastas `fato_pedidos/data_pedido=.../` e os arquivos Parquet.
5. No DynamoDB, abra `prova-bigdata-6325032-execucoes`. Para uma execução bem-sucedida
   desse CSV, confira `linhas_lidas=110`, `linhas_gravadas=103`, `status=SUCESSO`,
   dataset, data/hora UTC e execution_id único. Guarde o print.

O workgroup grava resultados criptografados em `s3://<gold>/athena-results/` e limita
a leitura a 10 MiB por consulta, suficiente para este dataset. Alterações futuras
que removam datas podem deixar metadados de partição antigos: MSCK adiciona, mas não
remove partições. As consultas continuarão sem linhas nas localizações vazias;
para manter o catálogo limpo, remova essas partições explicitamente.

## Limpeza e entrega

Depois de salvar todas as evidências, em `infra/`:

```bash
terraform destroy
terraform state list
```

Guarde o print da destruição. Confirme no console que raw, gold, job, database,
workgroup e tabela DynamoDB foram removidos. O Glue pode criar logs no CloudWatch
fora do estado Terraform: apague apenas os streams desta execução se for necessária
a limpeza integral, sem remover logs de outras atividades.

No fork do repositório do professor, crie a branch `prova-6325032` e coloque esta
pasta em `entregas/6325032/`. Revise o CHECKLIST original, adicione as evidências AWS,
faça commit/push e abra PR para a `master` do repositório original.
Não inclua `.terraform/`, estados, planos, credenciais ou `terraform.tfvars`.
O arquivo `.terraform.lock.hcl` foi removido desta entrega porque estava travado
na versão errada do provider (6.66.0). Rode `terraform init` normalmente antes do
`apply` — ele vai gerar o lock file já na versão correta (5.31.0, fixada em
`versions.tf`) — e pode versionar o arquivo gerado.

## Relação com as aulas

S3 separa armazenamento do processamento e atende ao papel de repositório distribuído,
mas é armazenamento de objetos, não uma implementação de HDFS. Spark distribui as
transformações; Parquet e partições reduzem a leitura analítica. Nos 5 Vs, o projeto
exercita organização para Volume/Velocidade, um fluxo extensível para Variedade,
tratamento de inválidos para Veracidade e consultas de vendas para Valor. Este CSV
pequeno demonstra a arquitetura, sem afirmar que o volume do exercício é Big Data.
DynamoDB mantém itens de execução com chave simples; Athena usa SQL no esquema estrela.

## Referências técnicas

- [Versões do AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html)
- [Recurso Terraform aws_glue_job](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/glue_job)
- [MSCK REPAIR TABLE no Athena](https://docs.aws.amazon.com/athena/latest/ug/msck-repair-table.html)
