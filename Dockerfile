FROM tiangolo/uvicorn-gunicorn:python3.8-slim

LABEL maintainer="Koudai Aono <koxudaxi@gmail.com>"

ENV MODULE_NAME local_data_api.main
ENV MARIADB_CLIENT_VERSION 2.5.0
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk
ENV LD_LIBRARY_PATH /usr/lib/jvm/java-11-openjdk/jre/lib/amd64/server/


# This app supports only single process to share connections on workers
ENV WEB_CONCURRENCY 1

RUN  mkdir -p /usr/share/man/man1 \
     && apt-get update && apt-get install -y openjdk-11-jre libpq-dev  \
     && savedAptMark="$(apt-mark showmanual)" \
     && apt-get install -y gcc curl \
     && pip install JPype1==0.7.5 psycopg2==2.8.5\
     && curl -o /usr/lib/jvm/mariadb-java-client.jar \
        https://downloads.mariadb.com/Connectors/java/connector-java-${MARIADB_CLIENT_VERSION}/mariadb-java-client-${MARIADB_CLIENT_VERSION}.jar \
     && curl -o /usr/lib/jvm/postgresql-java-client.jar \
        https://jdbc.postgresql.org/download/postgresql-42.2.8.jar \
     && apt-mark auto '.*' > /dev/null \
     && apt-mark manual $savedAptMark \
     && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false  \
     && apt-get autoremove -y \
     && rm -rf /var/lib/apt/lists/*

COPY setup.py /app
COPY setup.cfg /app
COPY LICENSE /app
WORKDIR /app

RUN pip install .

COPY local_data_api /app/local_data_api
