FROM python:3.9-slim

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY crawling_async.py .

CMD [ "python", "crawling_async.py", "mongodb", "5" , "--crawling_robots=0"]