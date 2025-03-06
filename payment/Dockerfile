FROM python:3.12-slim

WORKDIR /home/payment
ENV PYTHONPATH=/home

COPY ./payment/requirements.txt ./requirements.txt

RUN pip install -r requirements.txt
RUN opentelemetry-bootstrap -a install

COPY ./common ./common
COPY ./payment .

EXPOSE 5000