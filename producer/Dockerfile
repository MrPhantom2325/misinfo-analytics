FROM python:3.11-slim

WORKDIR /app

COPY requirements/ /tmp/requirements/
COPY producer/ /app/

RUN pip install --no-cache-dir -r /tmp/requirements/producer.txt

CMD ["python", "producer.py"]