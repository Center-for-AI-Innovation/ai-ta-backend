FROM --platform=linux/amd64 python:3.10-slim as build

# Install dependencies
RUN apt update && \
    apt-get install -y \
        ffmpeg \
        tesseract-ocr && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN apt update && \
    apt-get install -y \
        libpq-dev \
        gcc \
        git-all && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest.py .
CMD ["python", "worker.py"]
