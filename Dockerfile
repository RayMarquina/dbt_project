FROM python:3.3

RUN pip install --upgrade pip && pip install dbt

WORKDIR /dbt
