import os
from dotenv import load_dotenv
from typing import List
from fastapi import FastAPI, Query
from google.cloud import bigquery
from api.models import CompanyStat, JobTitleStat, MajorStat
from api.bq import fetch_top_companies, fetch_top_job_titles, fetch_top_majors

# Load environment variables from .env file
load_dotenv()

app = FastAPI(
    title="Alumni Analytics API",
    description="Read-only analytics API over aggregated alumni metrics",
    version="0.1.0",
)


@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "service": "alumni-analytics-api",
    }

@app.get("/metrics/companies", response_model=List[CompanyStat])
def get_top_companies(limit: int = Query(10, ge=1, le=100)):
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BIGQUERY_DEMO_DATASET_ID")

    if not project_id or not dataset_id:
        raise RuntimeError("Missing GCP_PROJECT_ID or BIGQUERY_DEMO_DATASET_ID/BIGQUERY_DATASET_ID.")

    client = bigquery.Client()
    return fetch_top_companies(client=client, project_id=project_id, dataset_id=dataset_id, limit=limit)

@app.get("/metrics/job-titles", response_model=list[JobTitleStat])
def get_top_job_titles(limit: int = Query(10, ge=1, le=100)):
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BIGQUERY_DEMO_DATASET_ID") or os.getenv("BIGQUERY_DATASET_ID")

    if not project_id or not dataset_id:
        raise RuntimeError("Missing GCP_PROJECT_ID or BIGQUERY_DEMO_DATASET_ID/BIGQUERY_DATASET_ID.")

    client = bigquery.Client()
    return fetch_top_job_titles(client=client, project_id=project_id, dataset_id=dataset_id, limit=limit)

@app.get("/metrics/majors", response_model=list[MajorStat])
def get_top_majors(limit: int = Query(10, ge=1, le=100)):
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BIGQUERY_DEMO_DATASET_ID") or os.getenv("BIGQUERY_DATASET_ID")

    if not project_id or not dataset_id:
        raise RuntimeError("Missing GCP_PROJECT_ID or BIGQUERY_DEMO_DATASET_ID/BIGQUERY_DATASET_ID.")

    client = bigquery.Client()
    return fetch_top_majors(client=client, project_id=project_id, dataset_id=dataset_id, limit=limit)