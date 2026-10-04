SELECT
    COUNT(*) AS quantidade_pedidos,
    COUNT(DISTINCT data_pedido) AS quantidade_datas,
    ROUND(SUM(valor_total), 2) AS total_vendas
FROM fato_pedidos;
