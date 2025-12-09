FROM fraud-base:py311-xgb
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential librdkafka-dev ca-certificates libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# ⭐ SUPER IMPORTANT — solves long builds
RUN python -m pip install --upgrade pip setuptools wheel
RUN pip install --upgrade --prefer-binary cmake
ENV PIP_DEFAULT_TIMEOUT=200

COPY consumer-requirements.txt /app/consumer-requirements.txt
RUN pip install --no-cache-dir --prefer-binary -r /app/consumer-requirements.txt

COPY consumer.py /app/consumer.py
COPY alert_email.py /app/alert_email.py

RUN mkdir -p /app/checkpoints /app/data
VOLUME ["/app/checkpoints","/app/data","/app/model"]

CMD ["python", "/app/consumer.py"]
