# Arquivo de contexto para ferramenta de IA. #

## Contexto ##
Estamos em um ambiente acadêmico. Nosso objetivo é aplicar todos os conceitos que aprendi durante as aulas de Big Data deste semestre, que são:
- 5 Vs (Volume, Velocidade, Variedade, Veracidade, Valor)
- Armazenamento distribuído (HDFS/Hadoop)
- Armazenamento e processamento distribuídos
- Modelagem NoSQL
- Consulta analítica
- Infraestrutura como código

## Arquitetura Alvo ##
O fluxo de dados é: S3 raw (desnormalizado) → Glue Job PySpark (normalização) → S3 gold (Parquet particionado) → Athena (SQL), com registro de metadados de cada execução no DynamoDB.

   UM ÚNICO terraform apply (pasta prova/infra/)
        │
        ├── infra/raw.tf  (PRONTO — camada raw)
        │        │ cria + faz upload
        │        ▼
        │   [ S3 Bucket_Raw ] ◀── Dataset_Exemplo (CSV)
        │        │ 1. leitura
        │        ▼
        └── infra/gold.tf (VOCÊ CONSTRÓI — camada gold)
                 │
        ┌─────────────────────────────────────────────────────────────┐
        │                                                             │
        │   [ Glue Job PySpark ] ── usa ──▶ (LabRole, ARN)             │
        │        │ 2. escreve Parquet particionado                     │
        │        ▼                                                     │
        │   [ S3 Bucket_Gold ] ── registra tabelas ─▶ [ Glue Catalog ] │
        │        │                                          │          │
        │        │ 3. grava metadados                       ▼          │
        │        ▼                                    [ Athena WG ]     │
        │   [ DynamoDB catálogo ]                          │ 4. SQL    │
        │                                                  ▼           │
        │                                          [ Resultados S3 ]   │
        └─────────────────────────────────────────────────────────────┘


## Fluxo de execução ##

1. Você configura as credenciais temporárias do Learner Lab e roda um único terraform apply em prova/infra/. Esse apply cria de uma vez a camada raw (pronta, definida em infra/raw.tf: Bucket_Raw + upload do Dataset_Exemplo) e a camada gold que você constrói (definida em infra/gold.tf: Bucket_Gold, Glue Job, Glue Data Catalog, Athena Workgroup e tabela DynamoDB).

2. Você dispara o Glue Job (console ou aws glue start-job-run) e acompanha o status.

3. O Job lê o raw, normaliza em fato + dimensões, grava Parquet particionado no gold e grava um item de metadados no DynamoDB.

4. Você executa as consultas Athena de referência e compara com os resultados esperados.

5. Você confere o item de metadados no DynamoDB.

6. Você roda terraform destroy ao final para remover todos os recursos.

## O que já vem pronto e o que vamos construir ##

| Camada | Quem entrega | Arquivo/Pasta | Conteúdo |
|--------|--------------|---------------|----------|
| Infra raw | **Professor — pronto** | `infra/raw.tf` | Camada raw pronta: cria o `Bucket_Raw` privado (via AWS CLI) e sobe o dataset |
| Dataset de exemplo | **Professor — pronto** | `dataset/` | `pedidos_desnormalizado.csv` (tabela ampla desnormalizada) |
| Infra gold | **Você — a construir** | `infra/gold.tf` | Camada gold (TODOs): S3 gold, Glue Job (LabRole), Athena, Glue Data Catalog, DynamoDB |
| Job de normalização | **Você — a construir** | `glue-job/` | Script PySpark `normaliza_pedidos.py` (esqueleto com TODOs) |
| Teste local | **Professor — pronto** | `local-test/` | Ambiente Docker (Java 21 + PySpark 3.5.1) para testar a normalização antes do Glue |


**Estrutura da pasta `prova/`:**

```
prova/
├── README.md                         # Este enunciado
├── RUBRICA.md                        # Critérios de correção (100 pontos)
├── CHECKLIST.md                      # Checklist pré-entrega
├── infra/                            # Infra unificada (um único terraform apply)
│   ├── raw.tf                        # PRONTO — camada raw (Bucket_Raw + upload do dataset)
│   └── gold.tf                       # VOCÊ PREENCHE (TODOs — camada gold)
├── dataset/                          # PRONTO — dataset versionado
│   └── pedidos_desnormalizado.csv
├── glue-job/                         # VOCÊ PREENCHE (esqueleto com TODOs)
│   └── normaliza_pedidos.py
└── local-test/                       # PRONTO — teste local do PySpark
```

> **Não altere** `infra/raw.tf`, `dataset/` nem `local-test/`. Sua entrega concentra-se em
> `infra/gold.tf` e `glue-job/`.