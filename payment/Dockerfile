FROM python:3.12-slim

WORKDIR /home/payment

COPY ./payment .
COPY ./common ./common
COPY ./payment/requirements.txt ./requirements.txt

ENV PYTHONPATH=/home

RUN pip install -r requirements.txt
RUN opentelemetry-bootstrap -a install

EXPOSE 5000