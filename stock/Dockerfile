FROM python:3.12-slim

WORKDIR /home/flask-app

COPY ./stock .
COPY ./common ./common
COPY ./payment/requirements.txt ./requirements.txt

RUN pip install -r requirements.txt
RUN opentelemetry-bootstrap -a install

EXPOSE 5000