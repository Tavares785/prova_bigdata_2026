-- faturamento_por_categoria.sql
-- Propósito: faturamento total por categoria de produto (via join com dim_produto).
-- Resultado esperado: uma linha por categoria; sem NULL em categoria (ausentes viraram DESCONHECIDO);
--                     soma das linhas = SUM(valor_total) do fato.
-- Req 8.1 / Rubrica 13.4.2

SELECT p.categoria, ROUND(SUM(f.valor_total), 2) AS faturamento
FROM fato_pedidos f JOIN dim_produto p ON f.produto_id = p.produto_id
GROUP BY p.categoria ORDER BY faturamento DESC;
