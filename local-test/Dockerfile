FROM python:3.10-slim

LABEL maintainer="Professor - Big Data UNIFAAT"
LABEL description="Ambiente de teste local da Prova - Java 21 + PySpark 3.5.1 (coerente com as Aulas 05/06)"

# Instalar Java (necessario para o Spark)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

WORKDIR /lab

# Copiar e instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar codigo dos testes locais
COPY . .

# Comando padrao: rodar os testes de propriedade da normalizacao
CMD ["pytest", "-v", "--tb=short"]
