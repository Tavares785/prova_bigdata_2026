-- integridade_referencial.sql
-- Propósito: verificar integridade referencial fato → dim_produto (bônus).
-- Resultado esperado: orfaos = 0 (nenhum produto_id do fato sem correspondente na dimensão).
-- Req 8.4 / Rubrica 13.4.5

SELECT COUNT(*) AS orfaos FROM fato_pedidos f
LEFT JOIN dim_produto p ON f.produto_id = p.produto_id
WHERE p.produto_id IS NULL;
