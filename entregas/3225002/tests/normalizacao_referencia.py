"""Shim — NAO e o gabarito.

A suite do professor faz `from normalizacao_referencia import ...`. Este modulo
tem esse nome so para satisfazer o import, e reexporta as funcoes do SEU
arquivo. Assim os testes avaliam a sua implementacao.
"""

from normaliza_pedidos import montar_metadados, normalizar  # noqa: F401
