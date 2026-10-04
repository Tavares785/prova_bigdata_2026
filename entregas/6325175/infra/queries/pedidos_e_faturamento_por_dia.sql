-- pedidos_e_faturamento_por_dia.sql
-- Propósito: pedidos e faturamento agregados por dia.
-- Resultado esperado: uma linha por data distinta (= número de partições data_pedido=);
--                     SUM(qtd_pedidos) = total de linhas do fato.
-- Req 8.3 / Rubrica 13.4.4

SELECT data_pedido, COUNT(*) AS qtd_pedidos, ROUND(SUM(valor_total), 2) AS faturamento_dia
FROM fato_pedidos GROUP BY data_pedido ORDER BY data_pedido;
