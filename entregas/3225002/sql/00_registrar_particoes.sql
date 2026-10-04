-- 00_registrar_particoes.sql
--
-- Registra no Glue Data Catalog as particoes que o Glue Job criou no S3.
-- Necessario porque o Athena so le particoes que existem no catalogo: os
-- diretorios data_pedido=YYYY-MM-DD aparecem no S3 assim que o Job grava, mas
-- o catalogo nao sabe deles sozinho.
--
-- NESTA ENTREGA isto NAO foi necessario: o `aws_glue_crawler.gold` declarado no
-- gold.tf faz esse trabalho automaticamente, e o README permite as duas vias
-- ("via Terraform ou Crawler com a LabRole").
--
-- Fica documentado como alternativa, para o caso de rodar sem o Crawler.

MSCK REPAIR TABLE fato_pedidos;

-- Conferencia — deve listar as 24 datas distintas:
-- SHOW PARTITIONS fato_pedidos;
