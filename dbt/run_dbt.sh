#!/bin/bash
# /opt/airflow/dbt/run_dbt.sh
# Wrapper script that Airflow calls to run dbt commands.
# Ensures correct environment and captures all output.

set -e

export HOME=/home/airflow
export PATH="/home/airflow/.local/bin:$PATH"

cd /opt/airflow/dbt

echo "=== dbt wrapper starting ==="
echo "Command: $@"
echo "DBT path: $(which dbt)"
echo "Working dir: $(pwd)"
echo "POSTGRES_HOST: $POSTGRES_HOST"
echo "==========================="

dbt "$@"

echo "=== dbt wrapper done ==="
