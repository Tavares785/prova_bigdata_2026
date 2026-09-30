SELECT
    p.categoria,
    ROUND(SUM(f.valor_total), 2) AS total_vendas
FROM fato_pedidos f
JOIN dim_produto p ON f.produto_id = p.produto_id
GROUP BY p.categoria
ORDER BY total_vendas DESC;
