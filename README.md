# TransitWatch

End-to-end data engineering pipeline for public transport delay analytics.

## Stack
- **Ingestion**: Python + GTFS Realtime API
- **Orchestration**: Apache Airflow
- **Transformation**: dbt
- **Storage**: PostgreSQL
- **Containerisation**: Docker Compose
- **CI/CD**: GitHub Actions

## Architecture
```
GTFS API → Python ingestion → PostgreSQL (raw)
                                    ↓
                          Airflow DAG (every 15 min)
                                    ↓
                          dbt: staging → marts
                                    ↓
                          PostgreSQL (analytics-ready)
```

## Quickstart
```bash
cp .env.example .env
docker-compose up -d
# Airflow UI → http://localhost:8080  (admin / admin)
# Postgres   → localhost:5432
```

## Project layout
```
transitwatch/
├── .github/workflows/   # CI/CD pipelines
├── ingestion/           # Python GTFS fetcher
├── dags/                # Airflow DAGs
├── dbt/                 # dbt models + tests
├── docker-compose.yml
├── init_db.sql          # raw schema
└── Makefile
```

## Development workflow
Every feature lives on a branch. Open a PR → CI runs automatically.
Merge to main → deploy runs automatically.
