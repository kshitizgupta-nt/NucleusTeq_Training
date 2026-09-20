FROM apache/airflow:2.8.1

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk default-mysql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
