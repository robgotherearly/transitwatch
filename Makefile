# Makefile — TransitWatch dev shortcuts
# Usage: make <target>

.PHONY: help up down logs ps shell-postgres shell-airflow \
        init-db fetch test lint dbt-run dbt-test clean

# Default target
help:
	@echo ""
	@echo "  TransitWatch — available commands"
	@echo ""
	@echo "  make up              Start all services"
	@echo "  make down            Stop all services"
	@echo "  make logs            Tail logs from all services"
	@echo "  make ps              Show running containers"
	@echo ""
	@echo "  make shell-postgres  psql shell into Postgres"
	@echo "  make shell-airflow   bash shell into Airflow scheduler"
	@echo ""
	@echo "  make fetch           Run ingestion manually (one fetch)"
	@echo "  make test            Run Python unit tests"
	@echo "  make lint            Run flake8 + sqlfluff"
	@echo ""
	@echo "  make dbt-run         Run all dbt models"
	@echo "  make dbt-test        Run all dbt tests"
	@echo "  make dbt-docs        Generate + serve dbt docs"
	@echo ""
	@echo "  make clean           Remove all containers + volumes (destructive!)"
	@echo ""

# ---- Docker lifecycle ----------------------------------------

up:
	@cp -n .env.example .env 2>/dev/null || true
	@echo "Starting TransitWatch stack..."
	docker-compose up -d --build
	@echo ""
	@echo "  Airflow UI  → http://localhost:8080  (admin / admin)"
	@echo "  Postgres    → localhost:5432"
	@echo ""

down:
	docker-compose down

logs:
	docker-compose logs -f

ps:
	docker-compose ps

# ---- Shells --------------------------------------------------

shell-postgres:
	docker-compose exec postgres psql -U $$POSTGRES_USER -d $$POSTGRES_DB

shell-airflow:
	docker-compose exec airflow-scheduler bash

# ---- Pipeline actions ----------------------------------------

fetch:
	docker-compose run --rm ingestion python fetch_gtfs.py

test:
	docker-compose run --rm ingestion pytest tests/ -v

lint:
	docker-compose run --rm ingestion flake8 . --max-line-length=100
	docker-compose run --rm airflow-scheduler \
	  sqlfluff lint /opt/airflow/dbt/models --dialect ansi

# ---- dbt -----------------------------------------------------

dbt-run:
	docker-compose exec airflow-scheduler \
	  bash -c "cd /opt/airflow/dbt && dbt run --profiles-dir ."

dbt-test:
	docker-compose exec airflow-scheduler \
	  bash -c "cd /opt/airflow/dbt && dbt test --profiles-dir ."

dbt-docs:
	docker-compose exec airflow-scheduler \
	  bash -c "cd /opt/airflow/dbt && dbt docs generate --profiles-dir . && dbt docs serve --port 8081"

# ---- Cleanup -------------------------------------------------

clean:
	@echo "WARNING: This will delete all data volumes."
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ]
	docker-compose down -v --remove-orphans
	@echo "Clean done."
