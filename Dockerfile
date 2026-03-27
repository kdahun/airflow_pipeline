FROM apache/airflow:2.9.0-python3.8

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.9.0" -r /requirements.txt
