# fastapi/Dockerfile
FROM fraud-base:py311-xgb
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip setuptools wheel
RUN pip install --upgrade --prefer-binary cmake
ENV PIP_DEFAULT_TIMEOUT=200

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --prefer-binary -r /app/requirements.txt

COPY app.py /app/app.py
COPY alert_email.py /app/alert_email.py

RUN mkdir -p /app/data
VOLUME ["/app/data","/app/model"]

EXPOSE 8080

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080", "--log-level", "info"]
