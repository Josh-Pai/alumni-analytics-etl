from enum import Enum
from pydantic import BaseModel, Field
from typing import Union


class CompanyStat(BaseModel):
    company_name: str
    alumni_count: int

class JobTitleStat(BaseModel):
    job_title: str
    job_count: int

class MajorStat(BaseModel):
    major: str
    major_count: int


class MetricsIntent(str, Enum):
    top_companies = "top_companies"
    top_job_titles = "top_job_titles"
    top_majors = "top_majors"
    unsupported = "unsupported"

class NLQRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=500)

class NLQResult(BaseModel):
    intent: MetricsIntent
    limit: int = Field(10, ge=1, le=100)
