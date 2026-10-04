-- Execute separadamente cada instrução no Athena.
SELECT COUNT(*) AS linhas_fato, COUNT(DISTINCT pedido_id) AS pedidos_unicos,
       ROUND(SUM(valor_total), 2) AS faturamento_total
FROM fato_pedidos;

SELECT COUNT(*) AS clientes_orfaos
FROM fato_pedidos f
LEFT JOIN dim_cliente c ON f.cliente_id = c.cliente_id
WHERE c.cliente_id IS NULL;

SELECT data_pedido, "$path" AS arquivo_parquet
FROM fato_pedidos
LIMIT 10;
