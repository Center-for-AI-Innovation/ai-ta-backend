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

# Pre-download NLTK data so unstructured (Excel, PPTX, etc.) does not fetch at runtime (avoids 403)
ENV NLTK_DATA=/usr/share/nltk_data
RUN mkdir -p $NLTK_DATA && chmod -R 755 $NLTK_DATA \
    && python -m nltk.downloader -d $NLTK_DATA punkt_tab averaged_perceptron_tagger_eng

EXPOSE 8001

CMD ["python", "worker.py"]
