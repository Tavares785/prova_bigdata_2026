#!/usr/bin/env bash
# Roda os testes do professor contra a SUA implementacao.
#
# IMPORTANTE: edite sempre o arquivo do REPO DA PROVA:
#     prova_bigdata_2026/glue-job/normaliza_pedidos.py
# A copia local desta pasta e descartavel e sobrescrita a cada execucao.
#
#   ./testar.sh              -> todos os testes
#   ./testar.sh -k borda     -> so os testes de borda
set -euo pipefail

AQUI="$(cd "$(dirname "$0")" && pwd)"
PROVA="$AQUI/../prova_bigdata_2026"
FONTE="$PROVA/entregas/3225002/glue-job/normaliza_pedidos.py"

# Guarda-costas: se a copia local tiver mudancas que NAO estao na fonte, avisa
# antes de sobrescrever. Foi assim que se perdeu trabalho uma vez.
if [ -f "$AQUI/normaliza_pedidos.py" ] && ! diff -q "$AQUI/normaliza_pedidos.py" "$FONTE" > /dev/null 2>&1; then
  BKP="$AQUI/normaliza_pedidos.$(date +%Y%m%d-%H%M%S).bak.py"
  cp "$AQUI/normaliza_pedidos.py" "$BKP"
  echo "AVISO: a copia local estava diferente do repo da prova."
  echo "       Backup salvo em: $(basename "$BKP")"
  echo "       Edite sempre: prova_bigdata_2026/glue-job/normaliza_pedidos.py"
  echo
fi

cp "$PROVA/local-test/test_normalizacao.py" "$AQUI/"
cp "$PROVA/local-test/requirements.txt"     "$AQUI/"
cp "$FONTE"                                 "$AQUI/"

docker build -q -t prova-meus-testes "$AQUI" > /dev/null
docker run --rm prova-meus-testes pytest -v --tb=short "$@"
