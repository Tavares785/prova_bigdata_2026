FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
ENV PYSPARK_PYTHON=python3 SPARK_LOCAL_IP=127.0.0.1 PYTHONUNBUFFERED=1
WORKDIR /prova
COPY tests/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY glue-job/ glue-job/
COPY dataset/ dataset/
COPY tests/ tests/
COPY sql/ sql/
CMD ["python", "-m", "pytest", "-v", "--tb=short", "tests/"]
