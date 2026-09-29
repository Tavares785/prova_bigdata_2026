-- top5_clientes.sql
-- Propósito: top 5 clientes por gasto total (via join com dim_cliente).
-- Resultado esperado: no máximo 5 linhas; ordenado por gasto_total decrescente.
-- Req 8.2 / Rubrica 13.4.3

SELECT c.cliente_nome, ROUND(SUM(f.valor_total), 2) AS gasto_total
FROM fato_pedidos f JOIN dim_cliente c ON f.cliente_id = c.cliente_id
GROUP BY c.cliente_nome ORDER BY gasto_total DESC LIMIT 5;
