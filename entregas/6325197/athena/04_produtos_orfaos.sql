SELECT COUNT(*) AS produtos_orfaos
FROM fato_pedidos f
LEFT JOIN dim_produto p ON f.produto_id = p.produto_id
WHERE p.produto_id IS NULL;
