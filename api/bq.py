from __future__ import annotations
from typing import List
from google.cloud import bigquery
from api.models import CompanyStat, JobTitleStat, MajorStat


def fetch_top_companies(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    limit: int = 10,
) -> List[CompanyStat]:
    """
    Fetch top companies from BigQuery aggregated table: stats_company.

    Notes:
    - Read-only
    - Bounded by hard cap (max 100)
    - Uses parameterized query for safety
    """
    limit = max(1, min(limit, 100))
    
    query = f"""
        SELECT company_name, alumni_count
        FROM `{project_id}.{dataset_id}.stats_company`
        ORDER BY alumni_count DESC
        LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )

    rows = client.query(query, job_config=job_config).result()


    return [
        CompanyStat(
            company_name=row["company_name"],
            alumni_count=int(row["alumni_count"]),
        )
        for row in rows
    ]

def fetch_top_job_titles(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    limit: int = 10,
) -> list[JobTitleStat]:
    """
    Fetch top job titles from BigQuery aggregated table: stats_job_title.
    """
    limit = max(1, min(limit, 100))

    query = f"""
        SELECT job_title, job_count
        FROM `{project_id}.{dataset_id}.stats_job_title`
        ORDER BY job_count DESC
        LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )

    rows = client.query(query, job_config=job_config).result()

    return [
        JobTitleStat(
            job_title=row["job_title"],
            job_count=int(row["job_count"]),
        )
        for row in rows
    ]

def fetch_top_majors(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    limit: int = 10,
) -> list[MajorStat]:
    """
    Fetch top majors from BigQuery aggregated table: stats_major.
    """
    limit = max(1, min(limit, 100))

    query = f"""
        SELECT major, major_count
        FROM `{project_id}.{dataset_id}.stats_major`
        ORDER BY major_count DESC
        LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )

    rows = client.query(query, job_config=job_config).result()

    return [
        MajorStat(
            major=row["major"],
            major_count=int(row["major_count"]),
        )
        for row in rows
    ]