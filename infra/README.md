# infra — Terraform da prova

Uma única pasta, um único `terraform apply`. Aqui vive a infraestrutura da prova.

- **`raw.tf` — ✅ PRONTO (não altere).** Cria o Bucket_Raw (privado) e sobe o
  `dataset/pedidos_desnormalizado.csv`. É a camada de origem, entregue pronta.
- **Camada gold e demais recursos — ✍️ VOCÊ CONSTRÓI DO ZERO.** Bucket gold,
  Glue Job, Glue Data Catalog (+ Crawler), Athena e DynamoDB. Vocês têm aula de
  Terraform: a estrutura NÃO vem pronta — crie os arquivos `.tf`, variáveis e
  outputs que precisar por conta própria.

> ℹ️ Só a raw é entregue pronta. Cabe a você desenhar e escrever o restante da
> infraestrutura (arquivos, variáveis, outputs, dependências). Faz parte da
> avaliação você estruturar isso como faria num trabalho real.

## Por que uma pasta só?

No AWS Academy Learner Lab **cada aluno tem a própria conta**. Não dá para o
professor provisionar o raw antecipadamente na sua conta — então a camada raw
precisa nascer aqui também. Junte a sua infra à raw e rode **um `apply` só**.

## Pré-requisitos

- Sessão do **Learner Lab** ativa e credenciais temporárias exportadas
  (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`) — região
  `us-east-1`.
- **AWS CLI v2** instalado (os buckets são criados via CLI — ver aviso abaixo).
- **Terraform >= 1.5**.

> ⚠️ Os buckets NÃO usam o recurso `aws_s3_bucket`. A SCP do Learner Lab nega
> `s3:GetBucketObjectLockConfiguration`, que esse recurso sempre lê. Por isso os
> buckets são criados via AWS CLI dentro do `terraform apply` (padrão
> `terraform_data` + `local-exec`). Mantenha o AWS CLI instalado e autenticado.
> Use o `raw.tf` como referência do padrão ao construir o seu bucket gold.

## Como usar

```bash
cd prova/infra
cp terraform.tfvars.example terraform.tfvars
# edite terraform.tfvars com os valores das variáveis (a raw precisa de bucket_raw_nome)

terraform init
terraform validate
terraform plan
terraform apply        # cria a raw (pronta) + o que VOCÊ construir
```

Depois de construir o gold: suba o script do Glue, dispare o Glue Job
(`aws glue start-job-run --job-name <seu_glue_job>`), rode as consultas no
Athena e confira os metadados no DynamoDB.

## Limpeza (obrigatória)

```bash
terraform destroy      # remove tudo (os buckets somem pelos provisioners de destroy)
```

> O Learner Lab tem orçamento e tempo de sessão limitados — rode o `destroy` ao
> final e não deixe recursos residuais.
