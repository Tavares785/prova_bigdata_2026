#!/usr/bin/env bash
# Diz, em 1 segundo, o que ja esta implementado no arquivo QUE VALE.
AQUI="$(cd "$(dirname "$0")" && pwd)"
F="$AQUI/../glue-job/normaliza_pedidos.py"

python3 - "$F" <<'PY'
import re, sys, os, time
caminho = sys.argv[1]
print("arquivo :", os.path.realpath(caminho))
print("salvo em:", time.strftime('%d/%m %H:%M:%S', time.localtime(os.path.getmtime(caminho))))
print()
src = open(caminho, encoding='utf-8').read()
alvo = ["normalizar", "montar_metadados", "ler_raw", "escrever_gold", "gravar_metadados_dynamo"]
# separa o corpo de cada funcao de topo
blocos = re.split(r'^(def \w+)', src, flags=re.M)
corpos = {}
for i in range(1, len(blocos), 2):
    corpos[blocos[i][4:]] = blocos[i+1]
feitas = 0
for fn in alvo:
    c = corpos.get(fn, "")
    if not c:
        print(f"  [?] {fn:<26} funcao nao encontrada")
    elif "raise NotImplementedError" in c:
        print(f"  [ ] {fn:<26} ainda com NotImplementedError")
    else:
        linhas = len([l for l in c.splitlines() if l.strip() and not l.strip().startswith('#')])
        print(f"  [x] {fn:<26} implementada  (~{linhas} linhas)")
        feitas += 1
print()
print(f"  {feitas}/5 funcoes implementadas")
PY
