FROM apache/airflow:2.7.1-python3.10

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk && \
    apt update && apt install -y telnet && \
    apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

COPY jars/apache-spark-sql-connector.jar /opt/spark/external-jars/


USER airflow

RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark==4.0.0
RUN pip install pycountry

