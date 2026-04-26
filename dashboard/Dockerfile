FROM python:3.11-slim

WORKDIR /app

COPY requirements/ /tmp/requirements/
COPY dashboard/ /app/

RUN pip install --no-cache-dir -r /tmp/requirements/dashboard.txt

EXPOSE 8050

CMD ["python", "app.py"]