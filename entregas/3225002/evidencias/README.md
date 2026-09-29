# Evidências visuais — capturas de tela

**Aluno:** José Henrique Teixeira Luiz — **RA:** 3225002
**Execução:** 29/09/2026 · AWS Academy Learner Lab, conta `591657390055`, `us-east-1`

Capturas do console AWS e do terminal, na ordem do fluxo da prova. O relato
completo com os outputs em texto está em [`../EVIDENCIAS.md`](../EVIDENCIAS.md).

| # | Arquivo | O que comprova |
|---|---------|----------------|
| 1 | [`01-terraform-apply.png`](01-terraform-apply.png) | `Apply complete! Resources: 8 added` — e o `put-bucket-tagging` com as 5 tags de custo |
| 2 | [`02-glue-job-succeeded.png`](02-glue-job-succeeded.png) | Glue Job `Succeeded` em 1 m 45 s · Glue 4.0 · G.1X |
| 3 | [`03-s3-particoes.png`](03-s3-particoes.png) | `Objetos (24)` em `fato_pedidos/`, no formato `data_pedido=YYYY-MM-DD` |
| 4a | [`04a-athena-consulta1-sql.png`](04a-athena-consulta1-sql.png) | Consulta 1 no editor, workgroup `prova-bigdata-gold` |
| 4b | [`04b-athena-consulta1-resultado.png`](04b-athena-consulta1-resultado.png) | 7 categorias, `Eletronicos 30334.1` no topo, sem `DESCONHECIDO` |
| 5a | [`05a-athena-consulta2-sql.png`](05a-athena-consulta2-sql.png) | Consulta 2 no editor · `fato_pedidos` marcado como **Particionado** |
| 5b | [`05b-athena-consulta2-resultado.png`](05b-athena-consulta2-resultado.png) | Top 5 clientes, `Carla Nunes 6955.2` no topo |
| 6 | [`06-dynamodb-metadados.png`](06-dynamodb-metadados.png) | `linhas_gravadas 103` · `linhas_lidas 110` · `status SUCESSO` |
| 6a | [`06a-dynamodb-tabela.png`](06a-dynamodb-tabela.png) | O mesmo item, com `execution_id`, `data_hora` e `dataset` |
| 7 | [`07-terraform-destroy.png`](07-terraform-destroy.png) | `Destroy complete! Resources: 8 destroyed` |

## Dois detalhes que valem a leitura

**No print 7**, repare no `local-exec` apagando cada Parquet das 24 partições,
os resultados do Athena e o `jobs/normaliza_pedidos.py` **antes** do
`remove_bucket`. É o provisioner `when = destroy` em ação — sem ele o bucket
não estaria vazio e o `terraform destroy` falharia, porque esses objetos são
escritos pelo Glue e não são gerenciados pelo Terraform.

**No print 1**, o comando `put-bucket-tagging` aparece por extenso, com as cinco
tags de custo em JSON. O `terraform_data` não aceita o argumento `tags` por não
ser um recurso AWS, então elas vão pelo mesmo caminho da criação do bucket.

## As consultas 3 e 4

Não têm captura de tela: a 3 devolve 24 linhas e a 4 devolve um único zero —
pouco legíveis em imagem. Ambas estão documentadas com o resultado completo em
[`../EVIDENCIAS.md`](../EVIDENCIAS.md) e em [`../sql/`](../sql/), onde cada
arquivo traz o resultado obtido em comentário.

## Conta limpa ao final

```
buckets da prova   : (nenhum)
Glue jobs          : (nenhum)
Glue databases     : (nenhum)
DynamoDB           : (nenhuma)
Athena workgroups  : (só o primary)
```
