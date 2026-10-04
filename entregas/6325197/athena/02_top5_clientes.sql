SELECT
    c.cliente_nome,
    ROUND(SUM(f.valor_total), 2) AS total_compras
FROM fato_pedidos f
JOIN dim_cliente c ON f.cliente_id = c.cliente_id
GROUP BY c.cliente_nome
ORDER BY total_compras DESC
LIMIT 5;
