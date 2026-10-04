SELECT data_pedido, COUNT(*) AS qtd_pedidos,
       ROUND(SUM(valor_total), 2) AS faturamento_dia
FROM fato_pedidos
GROUP BY data_pedido
ORDER BY data_pedido;
