FROM python:3.11-slim-bookworm

# Java is required by Spark; procps provides `ps`, used by Spark's launcher scripts.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY data ./data

# PYTHONPATH is inherited by the Spark Python workers (needed by the UDFs).
ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python
