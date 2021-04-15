FROM openjdk:11.0.10-jre-slim-buster

RUN mkdir /app
RUN chown -R $APPLICATION_USER /app

COPY ./build/libs/local-data-api-*-all.jar /app/local-data-api.jar
WORKDIR /app
CMD ["java", "-server", "-XX:+UnlockExperimentalVMOptions", "-XX:+UseContainerSupport", "-XX:+UseG1GC", "-XX:MaxGCPauseMillis=100", "-XX:+UseStringDeduplication", "-jar", "local-data-api.jar", "-port=80"]