# syntax=docker/dockerfile:1.4

FROM apache/spark:4.0.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/apt/archives/*

ENV SPARK_HOME=/opt/spark
ENV SPARK_EXTRA_JARS_DIR=${SPARK_HOME}/jars

RUN curl -fsSL \
    "https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.36.1.jar" \
    -o ${SPARK_EXTRA_JARS_DIR}/spark-bigquery-connector.jar && \
    curl -fsSL \
    "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar" \
    -o ${SPARK_EXTRA_JARS_DIR}/gcs-connector.jar

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin:$PATH"

COPY requirements-spark.txt /tmp/requirements-spark.txt

RUN uv pip install --system -r /tmp/requirements-spark.txt

USER spark

WORKDIR /opt/spark