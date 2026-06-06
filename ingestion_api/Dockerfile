FROM python:3.12-slim

WORKDIR /app

# Dependencies first (layer cache).
COPY ingestion_api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Package code.
COPY ingestion_api /app/ingestion_api

EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=5s --retries=5 \
    CMD python -c "import urllib.request,sys; urllib.request.urlopen('http://localhost:8000/health'); " || exit 1

CMD ["uvicorn", "ingestion_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
