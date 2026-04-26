FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
	&& apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
	&& rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"

COPY requirements/ /tmp/requirements/
COPY spark/ /app/

RUN pip install --no-cache-dir -r /tmp/requirements/spark.txt

CMD ["python", "run_all.py"]