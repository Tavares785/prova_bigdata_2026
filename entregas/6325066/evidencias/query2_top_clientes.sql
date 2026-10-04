SELECT 
    c.cliente_nome,
    c.cliente_uf,
    COUNT(DISTINCT f.pedido_id) AS total_compras,
    SUM(f.valor_total) AS total_gasto
FROM db_pedidos_gold.fato_pedidos f
JOIN db_pedidos_gold.dim_cliente c 
    ON f.cliente_id = c.cliente_id
GROUP BY c.cliente_nome, c.cliente_uf
ORDER BY total_gasto DESC
LIMIT 10;
