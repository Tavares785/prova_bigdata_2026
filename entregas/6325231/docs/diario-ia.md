# Diário de trabalho

## P001 — 29/09/2026

Prompt: pedido anexado para concluir a prova prática de Big Data com especificações, implementação, testes, execução real e evidências; identificação informada: Andreyh Rodrigues de Souza, RA 6325231. O usuário pediu para ignorar o aviso de IA do checklist. Sem segredos no prompt.

Objetivo: construir pipeline S3 → Glue → S3 Parquet → Athena, com DynamoDB e Terraform, e entregar em branch própria.

Inspeção: fork `Andreyh117/prova-6325231`, branch inicial `master`; `README.md` já estava modificado pelo usuário. Criada `prova-6325231`. Nenhum PDF foi encontrado. Teste oficial importa o gabarito, então serão criados testes para o script entregue. CSV tem 110 linhas; 103 passam na filtragem básica de chaves e quantidade.

Arquivos: especificações e documentação inicial em `entregas/6325231/`. Validações AWS ainda pendentes.

## P002 — Dados complementares

Usuário confirmou que o projeto aberto contém as fontes disponíveis; Learner Lab ativo com US$ 50. Identidade AWS e LabRole confirmadas. Provider 5.31.0 oficial verificado por SHA256; init precisou de espelho local temporário porque havia outra versão em cache. Fmt, validate, plano com 11 criações e apply: sucesso. Testes do script: 3 passed. Job Glue SUCCEEDED em 98 s; 110 linhas lidas, 103 fatos, 24 partições. Athena Q1–Q4 e verificações adicionais passaram; detalhes em evidencias.md.

## P003 — Ajuste de escopo

Usuário pediu concisão, testes direcionados, sem agentes paralelos e restauração do README.md raiz. O arquivo foi restaurado. Prints e limpeza aguardam captura das telas reais.

Verificação de versões S3: a primeira consulta CLI falhou porque length(null) é inválido; consulta corrigida tratando arrays ausentes como vazios. Raw: 1 objeto; gold: 39 objetos; zero versões numeradas e zero marcadores. Plano de destruição: 11 exclusões exclusivas da prova.

## P004 — Retomada

Usuário pediu retomada. Evidências JSON e consultas foram salvas, commit b425082 criado. Plano de destruição revisado; prints reais ainda pendentes e recursos mantidos ativos.

## P005 — Nova sessão do Learner Lab

Usuário informou novas credenciais e saldo de US$ 50. STS confirmou conta 906975261211, diferente da primeira. LabRole e ausência de recursos preexistentes verificados. Criado workspace isolado; plano de 11 criações e apply concluídos. Glue e Athena repetidos nessa conta para permitir prints no console atual; IDs em evidencias.md. Estado antigo preservado; limpeza de ambas as contas pendente dos prints/acesso.

30/09/2026 — Prints reais salvos: apply da primeira conta, Athena Q1–Q6 e item DynamoDB da segunda. Destroy na conta 906975261211: Athena exigiu exclusão recursiva do workgroup; repetição concluiu. Estado vazio e recursos ausentes. Conta antiga sem acesso para verificar limpeza.
