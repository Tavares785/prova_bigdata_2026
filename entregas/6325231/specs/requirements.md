# Requisitos e aceitação — RA 6325231

Fonte: README.md, RUBRICA.md, CHECKLIST.md e esqueleto deste repositório. O PDF citado no pedido não estava disponível em 29/09/2026; qualquer divergência posterior exige revisão.

| Requisito | Implementação | Verificação | Evidência | Estado |
|---|---|---|---|---|
| Raw e gold em um apply; S3 privado | `infra/raw.tf` preservado; `infra/gold.tf` | `terraform validate`, plano e apply | `docs/evidencias.md` | Validado |
| Glue com LabRole e PySpark | `infra/gold.tf`, `glue-job/normaliza_pedidos.py` | teste local e JobRun SUCCEEDED | JobRunId | Validado |
| Fato + dimensões e dados inválidos | `normalizar` | testes próprios sobre o script entregue | resultados locais | Validado |
| Parquet e partição por data | `escrever_gold` | leitura e inspeção de partições | teste local e S3 | Validado |
| Catálogo e Athena, quatro consultas | Glue Catalog + Athena WG | Q1 a Q4 e órfãos de clientes | QueryExecutionIds; prints pendentes | Consultas validadas |
| Metadados por execução | DynamoDB + Job | testes e item real | item sanitizado; print pendente | Item validado |
| Segurança, custo e limpeza | tags, buckets privados, destroy | revisão de plano, identidade e resíduos | apply/destroy reais | Pendente |
| Entrega isolada | `entregas/6325231/`, branch e PR | diff, push e PR | links e commits | Em andamento |

Critérios de aceitação: 110 linhas de entrada no CSV versionado; fato só com chaves presentes e quantidade positiva; uma linha por `pedido_id`; dimensões únicas por chave e sem órfãos; textos ausentes preenchidos com `DESCONHECIDO`; saídas Parquet nos prefixos especificados; reexecução sem duplicação; metadados de SUCESSO/FALHA coerentes; recursos da prova removidos após capturar prints.

Diferenças observadas: o README menciona `infra/gold.tf` como TODO, mas o arquivo não existe; o teste oficial usa `local-test/normalizacao_referencia.py`, não o script do aluno; o README exige quatro consultas, enquanto o checklist fala em pelo menos três. Serão executadas as quatro. O README raiz foi restaurado ao estado original a pedido do usuário.
