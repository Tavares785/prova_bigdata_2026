# Bancada de testes do ALUNO.
#
# Ideia: a suite test_normalizacao.py do professor importa
#     from normalizacao_referencia import montar_metadados, normalizar
#
# Esta imagem NAO copia o gabarito. No lugar dele entra um shim de mesmo nome
# que reexporta as funcoes do SEU glue-job/normaliza_pedidos.py. Resultado: os
# 11 testes do professor passam a avaliar o SEU codigo.
#
# Nada na pasta local-test/ do repo e alterado.

FROM eclipse-temurin:21-jdk-jammy

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-pip \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /lab

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# A suite de testes do professor (sem o gabarito).
COPY test_normalizacao.py .

# O shim que redireciona para o seu codigo.
COPY normalizacao_referencia.py .

# O SEU arquivo, que e o que esta sendo testado de verdade.
COPY normaliza_pedidos.py .

CMD ["pytest", "-v", "--tb=short"]
