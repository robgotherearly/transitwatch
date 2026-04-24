# ingestion/Dockerfile
# Lightweight Python image for the GTFS fetcher.
# This image is also reused by Airflow workers to run tasks.

FROM python:3.11-slim

# Keeps Python from buffering stdout/stderr — important for Docker logs
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install system deps needed by psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default command — Airflow overrides this when running tasks
CMD ["python", "fetch_gtfs.py"]
