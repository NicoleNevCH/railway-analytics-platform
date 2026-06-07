# Streamlit dashboard for the Railway Analytics Platform.
#
# Talks to the two FastAPI services over HTTP. It also reuses the real chaos
# simulator package (copied in) so the "generate events" button injects the
# exact same defects as the CLI simulator.
FROM python:3.12-slim

WORKDIR /app

COPY dashboard/requirements.txt /app/dashboard/requirements.txt
RUN pip install --no-cache-dir -r /app/dashboard/requirements.txt

COPY dashboard /app/dashboard
# The simulator package is reused by the in-dashboard event generator.
COPY simulator /app/simulator

# So `from simulator.chaos_simulator import ...` resolves from /app.
ENV PYTHONPATH=/app

EXPOSE 8501
HEALTHCHECK --interval=15s --timeout=5s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')" || exit 1

CMD ["streamlit", "run", "/app/dashboard/app.py", \
     "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
