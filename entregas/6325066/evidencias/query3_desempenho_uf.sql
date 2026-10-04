SELECT 
    c.cliente_uf AS estado,
    COUNT(f.pedido_id) AS volume_de_pedidos,
    SUM(f.valor_total) AS receita_total
FROM db_pedidos_gold.fato_pedidos f
JOIN db_pedidos_gold.dim_cliente c 
    ON f.cliente_id = c.cliente_id
GROUP BY c.cliente_uf
ORDER BY receita_total DESC;
