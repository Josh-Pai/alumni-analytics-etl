from pydantic import BaseModel


class CompanyStat(BaseModel):
    company_name: str
    alumni_count: int

class JobTitleStat(BaseModel):
    job_title: str
    job_count: int

class MajorStat(BaseModel):
    major: str
    major_count: int