# Rubrica de Correção — Prova Prática de Big Data na AWS

Esta rubrica é usada pelo **Professor** para corrigir as entregas de forma objetiva,
justa e reproduzível. A pontuação total soma **100 pontos**, distribuídos entre 6
critérios. Cada critério referencia o requisito correspondente (`Requirement 13.x`).

> Escala: 0 a 100 pontos. Cada critério pode ser pontuado integralmente, parcialmente
> ou com zero, conforme a evidência apresentada na entrega (PR do aluno).

## Distribuição de pontos

| # | Critério | Pontos | Requirement |
|---|----------|-------:|-------------|
| 1 | Terraform (pasta `infra/`) aplica sem erro (`terraform apply` completo) | 20 | 13.1 |
| 2 | Job de normalização gera corretamente o Modelo Dimensional Alvo (fato + 2 dimensões) | 25 | 13.2 |
| 3 | Dados gravados no Bucket_Gold em Parquet particionado por `data_pedido` | 15 | 13.3 |
| 4 | Consultas Athena retornam os resultados esperados | 15 | 13.4 |
| 5 | Registro de metadados de execução no DynamoDB conforme o esquema definido | 10 | 13.5 |
| 6 | Custo/limpeza e segurança (tags, `destroy`, buckets privados, uso da LabRole) | 15 | 13.6 |
| | **Total** | **100** | 13.7 |

## Detalhamento dos critérios

### Critério 1 — Infraestrutura aplica sem erro (20 pts) · Requirement 13.1

Avalia se o Terraform da pasta `infra/` provisiona toda a infraestrutura sem falhas.

- **20 pts** — `terraform init`/`validate`/`apply` executam sem erro e criam todos os
  recursos esperados (Bucket_Gold, Glue Job, Glue Data Catalog, Athena Workgroup,
  tabela DynamoDB).
- **10–19 pts** — `apply` conclui, mas com recursos faltando ou avisos relevantes.
- **1–9 pts** — `apply` falha parcialmente; apenas parte da infra é criada.
- **0 pt** — infra não aplica ou não há evidência de `apply`.

### Critério 2 — Normalização correta (25 pts) · Requirement 13.2

Avalia se o Job de normalização produz o Modelo Dimensional Alvo (`fato_pedidos`,
`dim_cliente`, `dim_produto`) segundo as regras de negócio.

- **25 pts** — fato + 2 dimensões corretos: chaves das dimensões únicas, integridade
  referencial fato → dimensões, linhas inválidas descartadas (sem `pedido_id`/`cliente_id`/`produto_id`
  ou `quantidade <= 0`) e textos ausentes tratados como `DESCONHECIDO`.
- **13–24 pts** — normalização quase correta, com pequenas falhas de tratamento de nulos
  ou de deduplicação de dimensões.
- **1–12 pts** — modelo dimensional incompleto ou com erros estruturais graves.
- **0 pt** — sem normalização ou saída incorreta.

### Critério 3 — Parquet particionado (15 pts) · Requirement 13.3

Avalia a gravação no Bucket_Gold em formato Parquet particionado por `data_pedido`.

- **15 pts** — dados no gold em Parquet, particionados corretamente por `data_pedido`
  (uma partição por data distinta).
- **8–14 pts** — Parquet gravado, mas particionamento ausente ou incorreto.
- **1–7 pts** — gravação em formato diferente do especificado.
- **0 pt** — nada gravado no gold.

### Critério 4 — Consultas Athena (15 pts) · Requirement 13.4

Avalia se as consultas de referência no Athena retornam os resultados esperados sobre
os dados do Modelo Dimensional Alvo.

- **15 pts** — tabelas registradas no Glue Data Catalog e todas as consultas de
  referência retornam os resultados esperados (com evidência/print).
- **8–14 pts** — maioria das consultas correta; alguma falha ou resultado divergente.
- **1–7 pts** — catálogo/consultas parcialmente funcionais.
- **0 pt** — sem catálogo ou sem consultas executadas.

### Critério 5 — Metadados no DynamoDB (10 pts) · Requirement 13.5

Avalia o registro dos metadados de execução no DynamoDB conforme o esquema definido.

- **10 pts** — item gravado com o esquema esperado (`execution_id`, dataset,
  `linhas_lidas`, `linhas_gravadas`, status, etc.) e valores coerentes.
- **5–9 pts** — item gravado, mas com campos faltando ou valores incoerentes.
- **1–4 pts** — gravação parcial ou fora do esquema.
- **0 pt** — sem registro no DynamoDB.

### Critério 6 — Custo/limpeza e segurança (15 pts) · Requirement 13.6

Avalia boas práticas de custo, limpeza e segurança.

- **15 pts** — tags de custo em todos os recursos, buckets privados (public access
  block), uso da **LabRole** por ARN (sem criar roles/policies), credenciais nunca
  versionadas e evidência de `terraform destroy` ao final.
- **8–14 pts** — maioria das práticas atendida; uma ou duas ausências.
- **1–7 pts** — falhas relevantes de segurança/custo (ex.: bucket público, sem tags).
- **0 pt** — práticas de segurança/limpeza não atendidas.

## Total · Requirement 13.7

A soma dos 6 critérios totaliza **100 pontos**. O Professor pode reescalar para outra
base (ex.: 0–10), preservando os pesos relativos de cada critério.
