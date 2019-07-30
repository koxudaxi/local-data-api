FROM tiangolo/uvicorn-gunicorn:python3.7-alpine3.8

LABEL maintainer="Koudai Aono <koxudaxi@gmail.com>"

ENV MODULE_NAME local_data_api.main
ENV MARIADB_CLIENT_VERSION 2.4.2
ENV JAVA_HOME /usr/lib/jvm/java-1.8-openjdk/jre/
ENV LD_LIBRARY_PATH /usr/lib/jvm/java-1.8-openjdk/jre/lib/amd64/server/

RUN apk add --no-cache libstdc++ openjdk8-jre g++ libc-dev curl \
     &&  pip install JPype1==0.6.3 \
     && curl -o /usr/lib/jvm/mariadb-java-client.jar \
        https://downloads.mariadb.com/Connectors/java/connector-java-${MARIADB_CLIENT_VERSION}/mariadb-java-client-${MARIADB_CLIENT_VERSION}.jar \
     &&  apk del g++ libc-dev curl

COPY setup.py /app
COPY setup.cfg /app
COPY LICENSE /app
WORKDIR /app

RUN pip install .

COPY local_data_api /app/local_data_api
