FROM python:3.12-slim

WORKDIR /home/flask-app

COPY ./order/requirements.txt ./requirements.txt

RUN pip install -r requirements.txt
RUN opentelemetry-bootstrap -a install

COPY ./common ./common
COPY ./order .

EXPOSE 5000

