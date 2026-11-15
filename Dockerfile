FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# yt-dlp's JS challenge solver needs a real JS runtime — slim has none.
RUN apt-get update \
    && apt-get install -y --no-install-recommends nodejs ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY robin /app/robin
COPY songapi.py /app/songapi.py

ENV SONG_API_BIND=0.0.0.0 \
    PORT=10000

EXPOSE 10000
CMD ["python3", "-u", "songapi.py"]
