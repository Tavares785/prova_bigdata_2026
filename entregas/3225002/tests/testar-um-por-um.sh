#!/usr/bin/env bash
# Roda CADA teste no seu proprio container, com a JVM limitada.
#
# Por que: a suite inteira num processo so estoura a memoria da VM do Docker
# (3,8 GB, dos quais os stacks de trabalho ja consomem a maior parte) e a JVM do
# Spark e morta no meio. Ai TODOS os testes seguintes falham com
# ConnectionRefused — o que nao diz nada sobre o codigo.
#
# Um container por teste: cada um comeca com JVM limpa e morre em seguida.
set -uo pipefail

AQUI="$(cd "$(dirname "$0")" && pwd)"
PROVA="$AQUI/../prova_bigdata_2026"
FONTE="$PROVA/entregas/3225002/glue-job/normaliza_pedidos.py"

cp "$PROVA/local-test/test_normalizacao.py" "$AQUI/"
cp "$PROVA/local-test/requirements.txt"     "$AQUI/"
cp "$FONTE" "$AQUI/normaliza_pedidos.py"
docker build -q -t prova-meus-testes "$AQUI" > /dev/null

TESTES=$(grep -oE "^def (test_[a-z0-9_]+)" "$AQUI/test_normalizacao.py" | sed 's/^def //')

PASSOU=0; FALHOU=0; LISTA_FALHAS=""
for T in $TESTES; do
  printf '  %-58s ' "$T"
  SAIDA=$(docker run --rm \
      -e PYSPARK_SUBMIT_ARGS="--driver-memory 900m --conf spark.sql.shuffle.partitions=2 pyspark-shell" \
      prova-meus-testes pytest -q --no-header -p no:cacheprovider "test_normalizacao.py::$T" 2>&1)
  if grep -q "1 passed" <<< "$SAIDA"; then
    echo "PASSOU"; PASSOU=$((PASSOU+1))
  else
    MOTIVO=$(grep -oE "E   [A-Za-z]+(Error|Exception)[^$]*" <<< "$SAIDA" | head -1 | cut -c1-70)
    echo "FALHOU  ${MOTIVO:-(ver detalhe)}"
    FALHOU=$((FALHOU+1)); LISTA_FALHAS="$LISTA_FALHAS $T"
    echo "$SAIDA" > "$AQUI/falha-$T.log"
  fi
done

echo
echo "  ============================================"
printf '   %d passaram, %d falharam\n' "$PASSOU" "$FALHOU"
echo "  ============================================"
[ -n "$LISTA_FALHAS" ] && echo "  detalhe de cada falha em: falha-<nome_do_teste>.log"
