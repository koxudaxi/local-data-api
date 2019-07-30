#!/usr/bin/env bash

IMAGE=koxudaxi/local-data-api
VERSION=`python3 setup.py --version`

echo ${DOCKER_PASSWORD} | docker login -u ${DOCKER_USERNAME} --password-stdin

## build
docker build -t ${IMAGE}:${VERSION} .
docker tag ${IMAGE}:${VERSION} ${IMAGE}:latest

## push
docker push ${IMAGE}:${VERSION}
docker push ${IMAGE}:latest
