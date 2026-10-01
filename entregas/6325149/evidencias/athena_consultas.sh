#!/usr/bin/env bash
# Roda as 4 consultas de referência (README §8) no workgroup da prova e imprime os resultados.
# Uso: source ../../aws-creds.sh && ./athena_consultas.sh
set -euo pipefail
export AWS_DEFAULT_REGION=us-east-1
WG=prova-bigdata
DB=prova_bigdata

consulta() {
  local titulo="$1" sql="$2" id estado
  id=$(aws athena start-query-execution --work-group "$WG" \
    --query-execution-context Database="$DB" --query-string "$sql" \
    --query QueryExecutionId --output text)
  while :; do
    estado=$(aws athena get-query-execution --query-execution-id "$id" \
      --query QueryExecution.Status.State --output text)
    [[ $estado == SUCCEEDED || $estado == FAILED || $estado == CANCELLED ]] && break
    sleep 2
  done
  echo "=== $titulo ($estado)"
  echo "$sql"
  if [[ $estado == SUCCEEDED ]]; then
    aws athena get-query-results --query-execution-id "$id" --output json \
      --query 'ResultSet.Rows[*].Data[*].VarCharValue' | python3 -c \
      'import json,sys; [print(" | ".join(map(str,l))) for l in json.load(sys.stdin)]'
  else
    aws athena get-query-execution --query-execution-id "$id" \
      --query QueryExecution.Status.StateChangeReason --output text
  fi
  echo
}

consulta "Consulta 1 - Faturamento por categoria" \
  "SELECT p.categoria, ROUND(SUM(f.valor_total), 2) AS faturamento FROM fato_pedidos f JOIN dim_produto p ON f.produto_id = p.produto_id GROUP BY p.categoria ORDER BY faturamento DESC"
consulta "Consulta 2 - Top 5 clientes por gasto" \
  "SELECT c.cliente_nome, ROUND(SUM(f.valor_total), 2) AS gasto_total FROM fato_pedidos f JOIN dim_cliente c ON f.cliente_id = c.cliente_id GROUP BY c.cliente_nome ORDER BY gasto_total DESC LIMIT 5"
consulta "Consulta 3 - Pedidos e faturamento por dia" \
  "SELECT data_pedido, COUNT(*) AS qtd_pedidos, ROUND(SUM(valor_total), 2) AS faturamento_dia FROM fato_pedidos GROUP BY data_pedido ORDER BY data_pedido"
consulta "Consulta 4 - Orfaos fato -> dim_produto" \
  "SELECT COUNT(*) AS orfaos FROM fato_pedidos f LEFT JOIN dim_produto p ON f.produto_id = p.produto_id WHERE p.produto_id IS NULL"
consulta "Extra CA3 - totais do fato" \
  "SELECT COUNT(*) AS linhas, SUM(valor_total) AS soma, COUNT(DISTINCT data_pedido) AS datas FROM fato_pedidos"
