-- 03_faturamento_dia.sql — Consulta 3 da Secao 8
--
-- Pedidos e faturamento por dia. Le diretamente a coluna de particao, sem
-- JOIN: e a consulta que comprova que o particionamento serve ao seu
-- proposito.
--
-- Esperado: uma linha por data_pedido distinta, coincidindo com as particoes
-- do gold; SUM(qtd_pedidos) = total de linhas de fato_pedidos.

SELECT data_pedido,
       COUNT(*)                        AS qtd_pedidos,
       ROUND(SUM(valor_total), 2)      AS faturamento_dia
FROM fato_pedidos
GROUP BY data_pedido
ORDER BY data_pedido;

-- Resultado obtido (27/09/2026) — 24 linhas, as 6 primeiras:
--   2026-01-05   5   1499.0
--   2026-01-06   4    948.1
--   2026-01-07   4   1798.3
--   2026-01-08   4   2947.7
--   2026-01-09   4   1167.1
--   2026-01-10   4   3106.7
--
--   SUM(qtd_pedidos) = 103 = total de linhas do fato   OK
--   24 datas distintas   = 24 particoes no S3          OK
