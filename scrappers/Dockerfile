FROM python:3.9-slim

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY crawling_async.py .
COPY medias_per_countries.csv .

