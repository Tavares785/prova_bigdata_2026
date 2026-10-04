-- 05_validacoes_adicionais.sql
--
-- Consultas que NAO estao na Secao 8, mas que eu usei para conferir se o
-- resultado estava de fato correto. Sao elas que transformam "a consulta
-- rodou" em "o resultado esta certo".

-- ---------------------------------------------------------------------------
-- 1. As tres validacoes cruzadas que o README exige, numa consulta so
-- ---------------------------------------------------------------------------
SELECT (SELECT ROUND(SUM(valor_total), 2) FROM fato_pedidos)                     AS total_fato,
       (SELECT SUM(qtd)
          FROM (SELECT COUNT(*) AS qtd FROM fato_pedidos GROUP BY data_pedido))  AS soma_por_dia,
       (SELECT COUNT(DISTINCT data_pedido) FROM fato_pedidos)                    AS datas_distintas;

-- Resultado obtido:  61798.9 | 103 | 24
--
--   total_fato       tem de bater com a soma das categorias da Consulta 1
--   soma_por_dia     tem de bater com o total de linhas do fato
--   datas_distintas  tem de bater com o numero de particoes no S3


-- ---------------------------------------------------------------------------
-- 2. Grao do fato: uma linha por pedido
-- ---------------------------------------------------------------------------
SELECT COUNT(*)                   AS linhas,
       COUNT(DISTINCT pedido_id)  AS pedidos_distintos
FROM fato_pedidos;

-- Resultado obtido:  103 | 103
-- Iguais => nao ha pedido repetido no fato.


-- ---------------------------------------------------------------------------
-- 3. Chaves unicas nas dimensoes (o que o test_prop2 cobra)
-- ---------------------------------------------------------------------------
SELECT 'dim_cliente' AS tabela,
       COUNT(*)                    AS linhas,
       COUNT(DISTINCT cliente_id)  AS chaves_distintas
FROM dim_cliente
UNION ALL
SELECT 'dim_produto',
       COUNT(*),
       COUNT(DISTINCT produto_id)
FROM dim_produto;

-- Resultado obtido:  dim_cliente 21 | 21   e   dim_produto 8 | 8
-- linhas = chaves_distintas => nenhuma chave duplicada.


-- ---------------------------------------------------------------------------
-- 4. Integridade referencial pelo lado do CLIENTE
--    (a Consulta 4 da Secao 8 so testa o lado do produto)
-- ---------------------------------------------------------------------------
SELECT COUNT(*) AS orfaos_cliente
FROM fato_pedidos f
LEFT JOIN dim_cliente c ON f.cliente_id = c.cliente_id
WHERE c.cliente_id IS NULL;

-- Resultado obtido:  0


-- ---------------------------------------------------------------------------
-- 5. Onde o DESCONHECIDO apareceu de fato
-- ---------------------------------------------------------------------------
SELECT cliente_id, cliente_nome, cliente_uf
FROM dim_cliente
WHERE cliente_nome = 'DESCONHECIDO' OR cliente_uf = 'DESCONHECIDO';

-- Resultado obtido:
--   CLI018 | DESCONHECIDO  | MG
--   CLI019 | Tatiana Sousa | DESCONHECIDO
--
-- Duas linhas, e so essas. Note que o PRD084 e o PRD072, que tambem tinham
-- campos vazios no CSV, NAO aparecem na dim_produto como DESCONHECIDO: eles
-- se repetem no dataset e o first(ignorenulls=True) recuperou o valor real
-- das outras linhas. O CLI018 aparece uma unica vez, e nessa unica linha o
-- nome esta ausente — nao havia de onde recuperar.
