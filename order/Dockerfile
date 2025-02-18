FROM python:3.12-slim

WORKDIR /home/flask-app
ENV PYTHONPATH=/home/flask-app

#COPY ./requirements.txt .
#COPY . .
COPY ./otel /home/flask-app/otel
COPY ./order /home/flask-app


RUN pip install -r requirements.txt


EXPOSE 5000