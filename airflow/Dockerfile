FROM apache/airflow:3.3.0

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    dbt-core==1.11.12 \
    dbt-spark==1.10.3 \
    pyspark==3.5.7 \
    PyHive \
    thrift \
    thrift-sasl