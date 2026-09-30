# Entrega da Prova Prática de Big Data — RA 6325171
**Aluno:** Nicolas de Jesus Silva  
**Disciplina:** Big Data — UNIFAAT  

---

## 📸 Evidências de Execução e Validação

### 1. Consultas Analíticas no AWS Athena (Camada Gold)

#### Consulta 1 — Faturamento por Categoria
![Faturamento por Categoria](./evidencias/01_faturamento_categoria.png)

#### Consulta 2 — Top 5 Clientes por Gasto Total
![Top 5 Clientes](./evidencias/02_top5_clientes.png)

#### Consulta 3 — Pedidos e Faturamento por Dia (Particionado)
![Faturamento Diario](./evidencias/03_faturamento_diario.png)

#### Consulta 4 — Verificação de Integridade Referencial (Órfãos = 0)
![Integridade Referencial](./evidencias/04_integridade_orfaos.png)

---

### 2. Catálogo de Metadados no DynamoDB

Auditoria de execução do Glue Job gravada na tabela `execucoes`:
![DynamoDB Scan](./evidencias/05_dynamodb_scan.png)