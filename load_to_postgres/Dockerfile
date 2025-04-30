FROM python:3.9.13-slim

COPY requirements.txt /opt

RUN pip install --no-cache-dir -r /opt/requirements.txt

RUN apt-get update && apt-get install -y --no-install-recommends && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app
COPY script.py /app
COPY load_ud_data.py /app 
WORKDIR /app

CMD python3 load_ud_data.py
