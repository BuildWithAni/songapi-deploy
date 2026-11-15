FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Only what songapi imports (platforms + config; heavy bot init never runs)
COPY robin /app/robin
COPY songapi.py /app/songapi.py

ENV SONG_API_BIND=0.0.0.0 \
    PORT=10000

EXPOSE 10000
CMD ["python3", "-u", "songapi.py"]
