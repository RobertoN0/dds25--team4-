FROM python:3.12-slim

WORKDIR /home/payment
ENV PYTHONPATH=/home

COPY ./payment/requirements.txt ./requirements.txt

RUN pip install -r requirements.txt
RUN opentelemetry-bootstrap -a install

COPY ./payment .
COPY ./common ./common

EXPOSE 5000