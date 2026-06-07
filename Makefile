# Railway Analytics Platform — operational shortcuts.
#
# Typical first run:
#     cp .env.example .env
#     make build         # build the images (Spark downloads the jars)
#     make up            # bring up the stack (MinIO, Spark, APIs, Jupyter)
#     make simulate      # generate events with chaos (needs httpx on the host)
#     make process       # run the Spark job (Bronze -> Silver/Iceberg)
#     make query         # print the punctuality KPIs (DuckDB)
#
# Or all at once (after `make up`):
#     make pipeline

COMPOSE ?= docker compose
PY      ?= python

# Simulator parameters (override: make simulate TRIPS=300)
TRIPS              ?= 120
UPDATES_PER_TRIP   ?= 3
NULL_RATE          ?= 0.12
TIME_MESS_RATE     ?= 0.10
DELAYED_RATE       ?= 0.35
SEED               ?= 42

.PHONY: help build up down restart ps logs init simulate process query status pipeline lab clean

help:
	@echo "Available targets:"
	@echo "  build     - build the Docker images"
	@echo "  up        - bring up the stack in the background (creates buckets automatically)"
	@echo "  down      - tear down the stack (keeps MinIO data)"
	@echo "  restart   - down + up"
	@echo "  ps        - container status"
	@echo "  logs      - follow the logs of all services"
	@echo "  init      - (re)create the MinIO buckets"
	@echo "  simulate  - generate events with chaos injection (host: needs httpx)"
	@echo "  process   - run the Spark job (Bronze -> Silver/Iceberg)"
	@echo "  query     - print the punctuality KPIs (consumption API)"
	@echo "  status    - print the pipeline status (Bronze/Silver/DLQ counts)"
	@echo "  pipeline  - simulate + process + query, end to end"
	@echo "  lab       - show the Jupyter URL"
	@echo "  clean     - tear down the stack AND delete the volumes (wipes data)"

build:
	$(COMPOSE) build

up:
	$(COMPOSE) up -d
	@echo "Dashboard    : http://localhost:8501"
	@echo "MinIO console : http://localhost:9001  (minioadmin / minioadmin)"
	@echo "Ingestion API : http://localhost:8000/docs"
	@echo "Consumption   : http://localhost:8001/docs"
	@echo "Spark master  : http://localhost:8080"
	@echo "Jupyter Lab   : http://localhost:8888  (token: railway)"

down:
	$(COMPOSE) down

restart: down up

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f

# Idempotent (re)creation of the buckets, in case it is needed outside of up.
init:
	$(COMPOSE) up minio-init

simulate:
	$(PY) -m simulator.chaos_simulator \
		--api-url http://localhost:8000 \
		--trips $(TRIPS) \
		--updates-per-trip $(UPDATES_PER_TRIP) \
		--null-rate $(NULL_RATE) \
		--time-mess-rate $(TIME_MESS_RATE) \
		--delayed-rate $(DELAYED_RATE) \
		--seed $(SEED)

process:
	$(COMPOSE) exec -T spark-master spark-submit /opt/bitnami/spark/jobs/bronze_to_silver.py

query:
	@$(PY) -c "import json,urllib.request; \
req=urllib.request.Request('http://localhost:8001/refresh', data=b'', method='POST'); \
urllib.request.urlopen(req); \
s=json.load(urllib.request.urlopen('http://localhost:8001/stats/punctuality')); \
print(json.dumps(s, indent=2, ensure_ascii=False))"

status:
	@$(PY) -c "import json,urllib.request; \
s=json.load(urllib.request.urlopen('http://localhost:8001/pipeline/status')); \
print(json.dumps(s, indent=2, ensure_ascii=False))"

pipeline:
	$(PY) orchestration/run_pipeline.py \
		--trips $(TRIPS) \
		--updates-per-trip $(UPDATES_PER_TRIP) \
		--null-rate $(NULL_RATE) \
		--time-mess-rate $(TIME_MESS_RATE) \
		--delayed-rate $(DELAYED_RATE) \
		--seed $(SEED)

lab:
	@echo "Jupyter Lab: http://localhost:8888  (token: railway)"

clean:
	$(COMPOSE) down -v
	find . -type d -name '__pycache__' -prune -exec rm -rf {} + 2>/dev/null || true
	@echo "Stack torn down and volumes removed."
