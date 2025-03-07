FROM python:3.12-slim

WORKDIR /home/flask-app

COPY ./orchestrator/requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
RUN opentelemetry-bootstrap -a install
COPY ./common ./common
COPY ./orchestrator .





EXPOSE 5000