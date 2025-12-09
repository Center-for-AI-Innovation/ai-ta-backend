FROM python:3.10-slim AS build

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

EXPOSE 8001

CMD ["python", "worker.py"]
