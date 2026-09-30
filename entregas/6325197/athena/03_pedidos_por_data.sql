SELECT
    data_pedido,
    COUNT(*) AS quantidade_pedidos,
    ROUND(SUM(valor_total), 2) AS total_vendas
FROM fato_pedidos
GROUP BY data_pedido
ORDER BY data_pedido;
