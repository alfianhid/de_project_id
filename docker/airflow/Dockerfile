# syntax=docker/dockerfile:1.4

FROM apache/airflow:3.1.8-python3.11

USER root

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/apt/archives/*

USER airflow

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/home/airflow/.local/bin:$PATH"
ENV UV_CACHE_DIR=/home/airflow/.cache/uv

COPY requirements-airflow.txt /tmp/requirements-airflow.txt

USER root
RUN /home/airflow/.local/bin/uv pip install --system -r /tmp/requirements-airflow.txt &&    /home/airflow/.local/bin/uv pip install --system psycopg2-binary
USER airflow

WORKDIR /opt/airflow