FROM tiangolo/uvicorn-gunicorn:python3.7-alpine3.8

LABEL maintainer="Koudai Aono <koxudaxi@gmail.com>"

ENV MODULE_NAME local_data_api.main
ENV MARIADB_CLIENT_VERSION 2.5.0
ENV JAVA_HOME /usr/lib/jvm/java-1.8-openjdk
ENV LD_LIBRARY_PATH /usr/lib/jvm/java-1.8-openjdk/jre/lib/amd64/server/


# This app supports only single process to share connections on workers
ENV WEB_CONCURRENCY 1

RUN apk add --no-cache libstdc++ openjdk8 g++ libc-dev curl gcc postgresql-dev \
     git && pip install git+git://github.com/jpype-project/jpype.git@96e2daf69fc2b93209133f513a6d742a90fe2b9f psycopg2==2.8.5\
     && curl -o /usr/lib/jvm/mariadb-java-client.jar \
        https://downloads.mariadb.com/Connectors/java/connector-java-${MARIADB_CLIENT_VERSION}/mariadb-java-client-${MARIADB_CLIENT_VERSION}.jar \
     && curl -o /usr/lib/jvm/postgresql-java-client.jar \
        https://jdbc.postgresql.org/download/postgresql-42.2.8.jar \
     &&  apk del g++ gcc libc-dev curl

COPY setup.py /app
COPY setup.cfg /app
COPY LICENSE /app
WORKDIR /app

RUN pip install .

COPY local_data_api /app/local_data_api
