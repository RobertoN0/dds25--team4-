FROM python:3.12-slim

WORKDIR /home/flask-app

COPY ./requirements.txt .

RUN pip install --upgrade pip
RUN pip install confluent_kafka
RUN pip install -r requirements.txt

COPY . .

EXPOSE 5000

