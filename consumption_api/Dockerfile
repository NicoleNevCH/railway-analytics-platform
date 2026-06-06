FROM python:3.12-slim

WORKDIR /app

# Dependencies first (layer cache).
COPY consumption_api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Package code.
COPY consumption_api /app/consumption_api

EXPOSE 8001
HEALTHCHECK --interval=15s --timeout=5s --retries=5 \
    CMD python -c "import urllib.request,sys; urllib.request.urlopen('http://localhost:8001/health'); " || exit 1

CMD ["uvicorn", "consumption_api.main:app", "--host", "0.0.0.0", "--port", "8001"]
