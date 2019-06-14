FROM tiangolo/uvicorn-gunicorn:python3.7-alpine3.8

MAINTAINER Koudai Aono (koxudaxi@gmail.com)

ENV MODULE_NAME local_data_api.main

COPY requirements.txt /
RUN pip install -r /requirements.txt

COPY local_data_api /app/local_data_api

