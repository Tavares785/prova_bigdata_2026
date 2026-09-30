-- Banco prova_6325231, workgroup prova-6325231.
-- Q1
SELECT p.categoria, ROUND(SUM(f.valor_total), 2) AS faturamento
FROM fato_pedidos f
JOIN dim_produto p ON f.produto_id = p.produto_id
GROUP BY p.categoria
ORDER BY faturamento DESC;

-- Q2
SELECT c.cliente_nome, ROUND(SUM(f.valor_total), 2) AS gasto_total
FROM fato_pedidos f
JOIN dim_cliente c ON f.cliente_id = c.cliente_id
GROUP BY c.cliente_nome
ORDER BY gasto_total DESC
LIMIT 5;

-- Q3
SELECT data_pedido, COUNT(*) AS qtd_pedidos,
       ROUND(SUM(valor_total), 2) AS faturamento_dia
FROM fato_pedidos
GROUP BY data_pedido
ORDER BY data_pedido;

-- Q4
SELECT COUNT(*) AS orfaos
FROM fato_pedidos f
LEFT JOIN dim_produto p ON f.produto_id = p.produto_id
WHERE p.produto_id IS NULL;

-- Verificação adicional
SELECT COUNT(*) AS orfaos_cliente
FROM fato_pedidos f
LEFT JOIN dim_cliente c ON f.cliente_id = c.cliente_id
WHERE c.cliente_id IS NULL;
