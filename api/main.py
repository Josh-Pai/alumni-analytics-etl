import os
from dotenv import load_dotenv
from typing import List
from fastapi import FastAPI, Query
from google.cloud import bigquery
from api.models import (
    CompanyStat,
    JobTitleStat,
    MajorStat,
    NLQRequest,
    NLQResult,
    MetricsIntent,
)
from api.bq import fetch_top_companies, fetch_top_job_titles, fetch_top_majors
from api.nlq_gemini import classify_intent

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
    dataset_id = os.getenv("BIGQUERY_DEMO_DATASET_ID")

    if not project_id or not dataset_id:
        raise RuntimeError("Missing GCP_PROJECT_ID or BIGQUERY_DEMO_DATASET_ID/BIGQUERY_DATASET_ID.")

    client = bigquery.Client()
    return fetch_top_job_titles(client=client, project_id=project_id, dataset_id=dataset_id, limit=limit)

@app.get("/metrics/majors", response_model=list[MajorStat])
def get_top_majors(limit: int = Query(10, ge=1, le=100)):
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BIGQUERY_DEMO_DATASET_ID")

    if not project_id or not dataset_id:
        raise RuntimeError("Missing GCP_PROJECT_ID or BIGQUERY_DEMO_DATASET_ID/BIGQUERY_DATASET_ID.")

    client = bigquery.Client()
    return fetch_top_majors(client=client, project_id=project_id, dataset_id=dataset_id, limit=limit)

@app.post("/nlq")
def nlq(req: NLQRequest):
    """
    Natural language metrics endpoint (demo).
    - Uses Gemini to classify the query into a bounded intent + limit.
    - Executes deterministic BigQuery queries (no PII).
    - Guardrails: schema-bounded output, fail-closed on errors, and demo rate limits.
    """
    result: NLQResult = classify_intent(req)

    # Fail-closed / out-of-scope path
    if result.intent == MetricsIntent.unsupported:
        return {
            "intent": result.intent.value,
            "limit": result.limit,
            "data": [],
            "message": "Unsupported or rate-limited query. Supported: companies, job titles, majors.",
        }

    # Deterministic execution path
    client = bigquery.Client()
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BIGQUERY_DEMO_DATASET_ID")

    if result.intent == MetricsIntent.top_companies:
        data = fetch_top_companies(client=client, project_id=project_id, dataset_id=dataset_id, limit=result.limit)
        return {"intent": result.intent.value, "limit": result.limit, "data": data}

    if result.intent == MetricsIntent.top_job_titles:
        data = fetch_top_job_titles(client=client, project_id=project_id, dataset_id=dataset_id, limit=result.limit)
        return {"intent": result.intent.value, "limit": result.limit, "data": data}

    if result.intent == MetricsIntent.top_majors:
        data = fetch_top_majors(client=client, project_id=project_id, dataset_id=dataset_id, limit=result.limit)
        return {"intent": result.intent.value, "limit": result.limit, "data": data}

    # Safety net (should not happen because intent is schema-bounded)
    return {
        "intent": MetricsIntent.unsupported.value,
        "limit": 10,
        "data": [],
        "message": "Unsupported query.",
    }