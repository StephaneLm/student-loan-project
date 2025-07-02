FROM bitnami/spark:3.5.1
COPY streaming/ /opt/bitnami/spark/jobs/streaming
COPY batch/ /opt/bitnami/spark/jobs/batch
