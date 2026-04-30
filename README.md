# TransitWatch 🚇

Real-time NYC subway delay analytics pipeline — an end-to-end data engineering project ingesting live MTA GTFS Realtime data, transforming it with dbt, orchestrating with Airflow, and auto-deploying to AWS EC2 via GitHub Actions CI/CD.

![CI](https://github.com/robgotherearly/transitwatch/actions/workflows/ci.yml/badge.svg)
![Deploy](https://github.com/robgotherearly/transitwatch/actions/workflows/deploy.yml/badge.svg)

---

## Architecture

![TransitWatch Architecture](./architecture.svg)

```
MTA GTFS Realtime API (9 subway feeds)
          │
          ▼
  Python ingestion service
  (protobuf parsing · retry logic · bulk insert)
          │
          ▼
  PostgreSQL — raw layer
  (raw_trip_updates · raw_vehicle_positions · raw_feed_fetches)
          │
          ▼
  Airflow DAG (every 15 minutes)
  ├── ingest_raw        → fetch all 9 feeds via MTA API
  ├── run_dbt_models    → raw → staging → marts
  ├── run_dbt_tests     → 23 automated data quality checks
  └── log_summary       → pipeline metrics via XCom
          │
          ▼
  PostgreSQL — mart layer
  ├── mart_route_delays         (per-route delay rankings)
  ├── mart_hourly_performance   (time-of-day analysis with LAG)
  └── mart_stop_hotspots        (worst stations via DENSE_RANK)
```

```
GitHub Push → CI (lint + pytest + dbt compile + docker build)
Merge to main → Deploy (build images → push ghcr.io → SSH → EC2 restart)
```

---

## Stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.11 · requests · gtfs-realtime-bindings · tenacity |
| Orchestration | Apache Airflow 2.9 (PythonOperator DAGs) |
| Transformation | dbt-postgres 1.7 |
| Storage | PostgreSQL 15 |
| Containerisation | Docker · Docker Compose |
| CI/CD | GitHub Actions |
| Cloud | AWS EC2 c7i-flex.large (2 vCPU, 4GB RAM) |
| Registry | GitHub Container Registry (ghcr.io) |

---

## Data model

### Raw layer (append-only — source of truth)
- `raw_feed_fetches` — log of every API call with status, count, duration, error
- `raw_trip_updates` — one row per stop per trip per fetch (arrival/departure delays)
- `raw_vehicle_positions` — live GPS positions, speed, bearing

### Staging layer (dbt views — cleaned + deduplicated)
- `stg_trip_updates` — ROW_NUMBER() dedup · null delays defaulted to 0 · dates parsed · MD5 surrogate key
- `stg_vehicle_positions` — invalid GPS filtered · unix timestamps converted

### Mart layer (dbt tables — analytics-ready)
- `mart_route_delays` — RANK() · percentile_cont(0.5/0.95) · delay_rate_pct per route
- `mart_hourly_performance` — LAG() for hour-over-hour delay change · worst route per hour
- `mart_stop_hotspots` — DENSE_RANK() within route and globally for worst stations

---

## CI/CD pipeline

Every pull request triggers 4 parallel jobs:
```
lint         → flake8 on ingestion/ and dags/
unit-tests   → pytest (20 tests, no network/DB needed)
dbt-validate → dbt deps + parse + compile against test DB
docker-build → builds ingestion + Airflow images
```

Every merge to main triggers:
```
build-and-push → builds and pushes images to ghcr.io
deploy         → SSH into EC2 → git pull → docker compose up
```

---

## Quickstart (local)

```bash
# 1. Clone
git clone https://github.com/robgotherearly/transitwatch.git
cd transitwatch

# 2. Create .env (never committed — see .env.example for required vars)
cp .env.example .env
# Edit .env — add your MTA API key (free at https://api.mta.info/)
# Generate Fernet key:
python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"

# 3. Start the stack
docker compose up -d --build

# 4. Open Airflow UI
# http://localhost:8081  (admin / admin)

# 5. Run ingestion manually
docker compose run --rm ingestion python fetch_gtfs.py

# 6. Run dbt
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt deps --profiles-dir . && dbt run --profiles-dir ."

# 7. Query delay analytics
docker compose exec postgres psql -U transitwatch -d transitwatch \
  -c "SELECT route_id, avg_delay_s, delay_rate_pct, delay_rank FROM public_marts.mart_route_delays ORDER BY delay_rank LIMIT 10;"
```

---

## Project layout

```
transitwatch/
├── .github/
│   └── workflows/
│       ├── ci.yml           # PR checks: lint + test + dbt + docker
│       └── deploy.yml       # main: build images + SSH deploy to EC2
├── ingestion/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── fetch_gtfs.py        # main ingestion — 9 MTA subway feeds
│   ├── gtfs_parser.py       # protobuf parsing (pure functions, fully tested)
│   ├── db.py                # database connection helpers
│   └── tests/
│       └── test_gtfs_parser.py   # 20 unit tests
├── dags/
│   ├── transit_pipeline.py  # main DAG (every 15 min, PythonOperator)
│   └── health_check.py      # monitoring DAG (daily freshness + null checks)
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── run_dbt.sh           # shell wrapper for dbt execution
│   └── models/
│       ├── staging/         # stg_trip_updates · stg_vehicle_positions
│       └── marts/           # mart_route_delays · mart_hourly_performance · mart_stop_hotspots
├── docker-compose.yml
├── Dockerfile.airflow       # custom Airflow image with dbt + permissions fix
├── init_db.sql              # raw + staging + mart schema
├── architecture.svg         # architecture diagram
└── .env.example             # environment template (never commit .env)
```

---

## Key engineering decisions

**Why PythonOperator instead of BashOperator for dbt?**
BashOperator swallows stdout/stderr in certain container configurations, making failures completely invisible (silent exit code 2). PythonOperator with `subprocess.run(capture_output=True)` gives full output visibility and proper error messages.

**Why default null delays to 0?**
97% of MTA GTFS records have null arrival_delay — the feed sends stop updates without delay info when a vehicle is on schedule. Dropping nulls leaves almost no mart data. Defaulting to 0 (on time) is the correct GTFS interpretation.

**Why chmod -R 777 on dbt folder in Dockerfile?**
Docker volumes inherit host file permissions. The dbt folder is mounted as root but Airflow runs as the airflow user, causing Permission Denied on logs and dbt_packages. Setting permissions at build time ensures correct access regardless of how the volume is mounted.

**Why AWS EC2 c7i-flex.large (4GB RAM)?**
Airflow webserver + scheduler + PostgreSQL together need at least 3-4GB RAM. t3.small (2GB) froze regularly under load. c7i-flex.large (4GB) runs comfortably and stays within free tier eligible instance categories.

---

## Sample analytics queries

```sql
-- Most delayed subway routes right now
SELECT route_id, avg_delay_s, p95_delay_s, delay_rate_pct, delay_rank
FROM public_marts.mart_route_delays
ORDER BY delay_rank;

-- What time of day is the subway worst?
SELECT hour_of_day, avg_delay_s, delayed_count, worst_route_id, delay_change_s
FROM public_marts.mart_hourly_performance
WHERE service_date = CURRENT_DATE
ORDER BY hour_of_day;

-- Worst stations today
SELECT stop_id, route_id, avg_delay_s, global_delay_rank
FROM public_marts.mart_stop_hotspots
WHERE service_date = CURRENT_DATE
ORDER BY global_delay_rank LIMIT 20;

-- Pipeline health — how reliable is ingestion?
SELECT feed_name,
       COUNT(*) AS total_runs,
       COUNT(*) FILTER (WHERE status = 'success') AS successes,
       ROUND(AVG(record_count)) AS avg_records
FROM raw_feed_fetches
WHERE fetched_at >= NOW() - INTERVAL '24 hours'
GROUP BY feed_name ORDER BY feed_name;
```

---

## Production notes

**Disk management:** Docker images accumulate on EC2. Run `docker system prune -a` periodically to free space. A full disk causes cryptic multi-service failures.

**dbt packages:** Run `dbt deps` inside the Airflow container after any rebuild to install dbt_utils.

**Environment variables:** Copy `.env.example` to `.env` and fill in values. `.env` is gitignored and must be created manually on each machine including EC2.

---

## Development workflow

```bash
# Create a feature branch
git checkout -b feature/your-feature

# Make changes, run tests locally
docker compose run --rm ingestion pytest tests/ -v

# Push and open a PR — CI runs automatically
git push origin feature/your-feature

# Merge to main → auto-deploys to EC2
```

---

## Environment variables

| Variable | Description |
|---|---|
| `POSTGRES_USER` | Database user |
| `POSTGRES_PASSWORD` | Database password |
| `POSTGRES_DB` | Database name |
| `MTA_API_KEY` | Free MTA API key (https://api.mta.info/) |
| `AIRFLOW__CORE__FERNET_KEY` | Airflow encryption key (generate with base64 + os.urandom) |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | Airflow web secret |
| `GTFS_FEED_NAME` | Feed identifier (nyc_mta) |