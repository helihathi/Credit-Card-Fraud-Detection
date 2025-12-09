# fraud-dashboard/Dockerfile
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

COPY realtime_dashboard.py /app/realtime_dashboard.py

RUN mkdir -p /app/data
VOLUME ["/app/data"]

EXPOSE 8501

CMD ["streamlit", "run", "/app/realtime_dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
