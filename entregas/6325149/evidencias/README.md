# Evidências (RA 6325149)

| Arquivo | O que comprova |
|---|---|
| `01-terraform-plan.txt` | plan: 11 recursos a criar (CA1) |
| `02-terraform-apply.txt` | apply completo: 11 criados (CA1) |
| `03-buckets-bloqueios-tags-dynamo-teste.txt` | 4 bloqueios nos 3 buckets, tags em buckets/DynamoDB/Glue, put-item manual (CA7) |
| `04-glue-runs-e-gold.txt` | 2 runs SUCCEEDED; gold com 24 partições e 1 arquivo por dimensão após o 2º run (CA2, CA3, RF9) |
| `05-athena-consultas.txt` | 4 consultas de referência + totais (CA4). Obs.: `61798.<conta>` é máscara indevida do ID da conta sobre a cauda decimal da soma (≈ 61798.9, igual à soma da consulta 1) |
| `06-dynamodb-scan.txt`, `06b-...resumo.txt` | itens com linhas_lidas=110, linhas_gravadas=103, SUCESSO (CA5) |
| `07-terraform-destroy.txt` | destroy: 11 removidos (CA8) |
| `08-verificacao-residuos.txt` | S3, Glue, Athena, DynamoDB e CloudWatch sem resíduos (CA8) |

Testes locais: `local-test/test_normaliza_aluno.py` (25 testes) — suíte completa 36/36 no Docker.
`athena_consultas.sh` roda as consultas no workgroup criado.
