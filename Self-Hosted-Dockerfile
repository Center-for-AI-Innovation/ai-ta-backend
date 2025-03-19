# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Install ffmpeg and git
RUN apt-get update && apt-get install -y \
    ffmpeg \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

RUN pip install uv

# Copy the requirements file first to leverage Docker cache
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN uv pip install -r requirements.txt --system

# Mkdir for sqlite db
RUN mkdir -p /usr/src/app/db

# Copy the rest of the local directory contents into the container
COPY . .

# Set the Python path to include the ai_ta_backend directory
ENV PYTHONPATH="${PYTHONPATH}:/usr/src/app/ai_ta_backend"

# Make port 8001 available to the world outside this container
EXPOSE 8001

# Run the application using Gunicorn with specified configuration
CMD ["gunicorn", "--workers=2", "--threads=3", "--worker-class=gthread", "ai_ta_backend.main:app", "--timeout=1800", "--bind=0.0.0.0:8001"]
