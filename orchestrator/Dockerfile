FROM python:3.12-slim

WORKDIR /home/flask-app

COPY ./orchestrator/requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
COPY ./common ./common
COPY ./orchestrator .





EXPOSE 5000