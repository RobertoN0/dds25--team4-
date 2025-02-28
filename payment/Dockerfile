FROM python:3.12-slim

WORKDIR /home/flask-app

COPY ./payment .
COPY ./common ./common

RUN pip install -r requirements.txt
RUN opentelemetry-bootstrap -a install

EXPOSE 5000