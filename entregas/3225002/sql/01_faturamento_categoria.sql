-- 01_faturamento_categoria.sql — Consulta 1 da Secao 8
--
-- Faturamento por categoria de produto.
-- Exercita o JOIN fato -> dim_produto.
--
-- Esperado: uma linha por categoria existente; faturamento = soma de
-- valor_total; SEM nulos em categoria (ausentes viraram DESCONHECIDO).
-- A soma de todas as linhas deve igualar SUM(valor_total) de fato_pedidos.

SELECT p.categoria,
       ROUND(SUM(f.valor_total), 2) AS faturamento
FROM fato_pedidos f
JOIN dim_produto p ON f.produto_id = p.produto_id
GROUP BY p.categoria
ORDER BY faturamento DESC;

-- Resultado obtido (27/09/2026):
--   Eletronicos       30334.1
--   Moveis            13485.0
--   Calcados           5698.1
--   Eletrodomesticos   4921.0
--   Livros             2876.8
--   Acessorios         2338.2
--   Vestuario          2145.7
--                    --------
--   soma              61798.9  = SUM(valor_total) do fato  OK
--
-- Nenhuma linha DESCONHECIDO: o PRD072 tinha a categoria ausente em uma das
-- doze linhas, e o groupBy + first(ignorenulls=True) da normalizacao
-- recuperou "Eletronicos" das outras onze.
