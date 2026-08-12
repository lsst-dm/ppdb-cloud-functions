FROM python:3.13-slim

WORKDIR /app

# Prevent Python from writing .pyc files and buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt ./

# Install Git to for dax-ppdb repository
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get purge -y --auto-remove git && \
    rm -rf /var/lib/apt/lists/*

COPY main.py ./

CMD ["python", "main.py"]
