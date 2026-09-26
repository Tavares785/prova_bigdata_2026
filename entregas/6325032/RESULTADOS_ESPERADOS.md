# Resultados esperados — dataset recebido

**Calculados localmente; não são resultados de uma execução no Athena.**

| Medida | Valor |
|---|---:|
| Linhas lidas | 110 |
| Linhas descartadas | 7 |
| Linhas no fato | 103 |
| Clientes | 16 |
| Produtos | 8 |
| Partições por data | 24 |
| Faturamento total | R$ 61.798,90 |

As 7 linhas rejeitadas são PED0096, PED0097, a linha sem pedido_id, PED0099, PED0100, PED0101 e PED0106.

## Consulta 1 — Faturamento por categoria

| Categoria | Faturamento (R$) |
|---|---:|
| Eletronicos | 30.334,10 |
| Moveis | 13.485,00 |
| Calcados | 5.698,10 |
| Eletrodomesticos | 4.921,00 |
| Livros | 2.876,80 |
| Acessorios | 2.338,20 |
| Vestuario | 2.145,70 |

O cadastro mais completo de PRD072 e PRD084 é preservado; por isso não aparece categoria DESCONHECIDO neste CSV. O preenchimento é testado com dados de borda.

## Consulta 2 — Top 5 clientes

| Cliente | Gasto total (R$) |
|---|---:|
| Carla Nunes | 6.955,20 |
| Diego Alves | 6.464,70 |
| Henrique Melo | 6.114,40 |
| Ana Souza | 5.793,30 |
| Elaine Rocha | 5.284,30 |

## Consulta 3 — Pedidos e faturamento por dia

| Data | Quantidade | Faturamento (R$) |
|---|---:|---:|
| 2026-01-05 | 5 | 1.499,00 |
| 2026-01-06 | 4 | 948,10 |
| 2026-01-07 | 4 | 1.798,30 |
| 2026-01-08 | 4 | 2.947,70 |
| 2026-01-09 | 4 | 1.167,10 |
| 2026-01-10 | 4 | 3.106,70 |
| 2026-01-11 | 4 | 2.898,20 |
| 2026-01-12 | 5 | 1.218,20 |
| 2026-01-13 | 4 | 5.196,80 |
| 2026-01-14 | 4 | 1.476,60 |
| 2026-01-15 | 4 | 1.838,20 |
| 2026-01-16 | 4 | 5.446,40 |
| 2026-01-17 | 4 | 937,40 |
| 2026-01-18 | 5 | 3.856,30 |
| 2026-01-19 | 4 | 3.318,20 |
| 2026-01-20 | 4 | 2.156,80 |
| 2026-01-21 | 5 | 2.947,50 |
| 2026-01-22 | 5 | 2.375,30 |
| 2026-01-23 | 5 | 1.448,30 |
| 2026-01-24 | 6 | 6.645,70 |
| 2026-01-25 | 3 | 849,50 |
| 2026-01-26 | 4 | 1.866,50 |
| 2026-01-27 | 4 | 2.478,40 |
| 2026-01-28 | 4 | 3.377,70 |

## Consulta 4 — Integridade

Órfãos de produto: **0**. Órfãos de cliente, na validação adicional: **0**.

## Metadados esperados

Uma execução bem-sucedida registra `linhas_lidas=110`, `linhas_gravadas=103`,
`status=SUCESSO` e `dataset=pedidos_desnormalizado`. `execution_id` e `data_hora`
devem ser obtidos da execução real, não copiados de um exemplo.
