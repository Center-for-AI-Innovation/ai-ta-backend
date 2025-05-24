FROM python:3.10-slim

# Install dependencies
RUN apt-get update && \
    apt-get install -y \
        ffmpeg \
        tesseract-ocr \
        libpq-dev  \
        gcc \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest.py .
CMD ["python", "worker.py"]
