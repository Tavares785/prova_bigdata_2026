SELECT 
    f.data_pedido,
    p.categoria,
    SUM(f.valor_total) AS faturamento_diario
FROM db_pedidos_gold.fato_pedidos f
JOIN db_pedidos_gold.dim_produto p 
    ON f.produto_id = p.produto_id
GROUP BY f.data_pedido, p.categoria
ORDER BY f.data_pedido ASC, faturamento_diario DESC;
