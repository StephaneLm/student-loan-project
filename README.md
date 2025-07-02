# Student‑Loan Project

## Quick start

```bash
# 1. Clone & enter repo
git clone <this_repo_url> && cd student-loan-project

# 2. Copy your CSV sources
mkdir -p data/raw/batch
cp /path/to/FL_Dashboard*.csv data/raw/batch/

# 3. Launch the whole stack
docker compose -f infrastructure/docker-compose.yml up -d

# 4. Initialise Airflow (first run)
docker compose -f infrastructure/docker-compose.yml run --rm airflow-init

# 5. Trigger the Kafka producer
docker compose -f infrastructure/docker-compose.yml run --rm producer

# 6. Watch jobs
open http://localhost:8088   # Airflow UI
open http://localhost:8080   # Spark Master UI
open http://localhost:3000   # Grafana
```

### Order of execution (automated by Airflow)

1. **Kafka Producer** streams rows → topic `student_loans_raw`
2. **Spark Structured Streaming** ingests and stores Delta Lake
3. **Spark Batch Metrics** job aggregates KPIs
4. **Power BI / Kylin** consume curated Parquet for reporting

## Tests

Run locally:

```bash
pip install -r producer/requirements.txt
pytest
```

## CI/CD

GitHub Actions workflow runs unit tests on each push to `main`.
