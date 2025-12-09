# producer/Dockerfile
FROM fraud-base:py311-xgb
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip setuptools wheel
RUN pip install --upgrade --prefer-binary cmake
ENV PIP_DEFAULT_TIMEOUT=200

COPY requirements-producer.txt /app/requirements.txt
RUN pip install --no-cache-dir --prefer-binary -r /app/requirements.txt

COPY producer.py /app/producer.py

VOLUME ["/app/data"]
CMD ["python", "/app/producer.py", "--mode", "dataset", "--db", "/app/data/fraud_detection.db", "--bootstrap", "kafka:9092"]
