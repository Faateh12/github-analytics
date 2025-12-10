# syntax=docker/dockerfile:1
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

# copy app + SQL files
COPY . .

# Cloud Run listens on $PORT
ENV PORT=8080
CMD ["gunicorn","-b","0.0.0.0:8080","app:app"]
