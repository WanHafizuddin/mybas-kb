# Airflow 3 + everything the basmy pipeline needs (Java for Spark, PySpark,
# dbt, DuckDB, BigQuery client). The pipeline's own dependencies live in a
# separate venv (/opt/basmy-venv) so they can never conflict with Airflow's.
# Build context is the repo root (see docker-compose.yml).
FROM apache/airflow:3.4.0-python3.11

USER root
RUN apt-get update -q \
 && apt-get install -y -q --no-install-recommends openjdk-21-jre-headless curl \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Cloud Storage connector so the Spark job can read gs:// raw data in production.
ARG GCS_CONNECTOR_VERSION=3.0.4
RUN mkdir -p /opt/spark-jars \
 && curl -fsSL -o /opt/spark-jars/gcs-connector.jar \
    https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/$GCS_CONNECTOR_VERSION/gcs-connector-$GCS_CONNECTOR_VERSION-shaded.jar

RUN python -m venv /opt/basmy-venv && chown -R airflow: /opt/basmy-venv
USER airflow
# Same dependency set as pyproject.toml extras [transform,warehouse].
RUN /opt/basmy-venv/bin/pip install --no-cache-dir \
      "requests>=2.32" "gtfs-realtime-bindings>=2.2,<3" \
      "pyspark==4.0.1" "pandas>=2.2" "pyarrow>=17" \
      "dbt-core>=1.10,<1.12" "dbt-duckdb>=1.10,<1.12" "dbt-bigquery>=1.10,<1.13" \
      "duckdb>=1.1" "google-cloud-bigquery>=3.25" "google-cloud-storage>=2.18"

# The repo itself is bind-mounted at /opt/basmy (see docker-compose.yml).
ENV BASMY_HOME=/opt/basmy \
    BASMY_PYTHON=/opt/basmy-venv/bin/python \
    BASMY_DBT=/opt/basmy-venv/bin/dbt
