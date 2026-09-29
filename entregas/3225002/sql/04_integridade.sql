-- 04_integridade.sql — Consulta 4 da Secao 8 (bonus)
--
-- Verificacao de integridade referencial fato -> dimensao.
-- Procura FKs do fato que nao existem na dimensao (orfaos).
--
-- Esperado: orfaos = 0.

SELECT COUNT(*) AS orfaos
FROM fato_pedidos f
LEFT JOIN dim_produto p ON f.produto_id = p.produto_id
WHERE p.produto_id IS NULL;

-- Resultado obtido (27/09/2026):  orfaos = 0   OK
--
-- Zero orfaos e consequencia direta da regra 6.7: a linha cujo produto_id era
-- ausente (PED0097) foi descartada do fato, e a linha de chave nula foi
-- descartada da dimensao. Se o fato mantivesse a linha sem produto_id, ela
-- apareceria aqui como orfa.

-- Mesma verificacao do lado do cliente:
-- SELECT COUNT(*) AS orfaos_cliente
-- FROM fato_pedidos f
-- LEFT JOIN dim_cliente c ON f.cliente_id = c.cliente_id
-- WHERE c.cliente_id IS NULL;
