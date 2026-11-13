# Local Spark runtime for the arrival-detection job (Phase 2).
# Build from the repo root:
#   docker build -f transform/Dockerfile -t basmy-transform .
FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_JARS_DIR=/opt/spark-jars

RUN apt-get update -q \
 && apt-get install -y -q --no-install-recommends openjdk-17-jre-headless curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Cloud Storage connector, so --raw/--out can be gs:// paths. Auth is
# Application Default Credentials mounted at runtime (see docker-compose.yml).
ARG GCS_CONNECTOR_VERSION=3.0.4
RUN mkdir -p $SPARK_JARS_DIR \
 && curl -fsSL -o $SPARK_JARS_DIR/gcs-connector.jar \
    https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/$GCS_CONNECTOR_VERSION/gcs-connector-$GCS_CONNECTOR_VERSION-shaded.jar

WORKDIR /app
COPY pyproject.toml ./
COPY basmy ./basmy
COPY collector ./collector
COPY transform ./transform
COPY tests ./tests
COPY data/gtfs_static ./data/gtfs_static
RUN pip install --no-cache-dir -e ".[transform,test]"

ENTRYPOINT ["python", "-m", "transform.arrivals_job"]
