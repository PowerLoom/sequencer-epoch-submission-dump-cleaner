FROM python:3.10.16-slim

RUN apt-get update && apt-get install -y \
    build-essential git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt

COPY . /app/

WORKDIR /app

CMD ["python3", "redis_epoch_cleanup.py"]
