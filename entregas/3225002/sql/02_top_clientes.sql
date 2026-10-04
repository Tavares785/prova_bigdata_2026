-- 02_top_clientes.sql — Consulta 2 da Secao 8
--
-- Top 5 clientes por gasto total.
-- Exercita o JOIN fato -> dim_cliente.
--
-- Esperado: no maximo 5 linhas, ordenadas de forma decrescente por
-- gasto_total; o cliente do topo com o maior somatorio de valor_total.

SELECT c.cliente_nome,
       ROUND(SUM(f.valor_total), 2) AS gasto_total
FROM fato_pedidos f
JOIN dim_cliente c ON f.cliente_id = c.cliente_id
GROUP BY c.cliente_nome
ORDER BY gasto_total DESC
LIMIT 5;

-- Resultado obtido (27/09/2026):
--   Carla Nunes     6955.2
--   Diego Alves     6464.7
--   Henrique Melo   6114.4
--   Ana Souza       5793.3
--   Elaine Rocha    5284.3
