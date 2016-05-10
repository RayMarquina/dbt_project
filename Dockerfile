FROM python:3.3

RUN pip install --upgrade pip && pip install dbt==0.1.14

WORKDIR /dbt
