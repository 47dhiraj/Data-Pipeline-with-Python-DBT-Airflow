FROM python:3.8-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1

ENV PYTHONBUFFERED 1

RUN apt-get update && apt-get install -y postgresql-client

COPY ./elt_script.py /app/elt_script.py

CMD ["python", "elt_script.py"]