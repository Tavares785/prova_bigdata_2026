SELECT 
    p.categoria,
    SUM(f.quantidade) AS total_itens_vendidos,
    SUM(f.valor_total) AS faturamento_total
FROM db_pedidos_gold.fato_pedidos f
JOIN db_pedidos_gold.dim_produto p 
    ON f.produto_id = p.produto_id
GROUP BY p.categoria
ORDER BY faturamento_total DESC;
