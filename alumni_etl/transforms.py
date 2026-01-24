import pandas as pd

# -----------------------------------------------------------------
# TRANSFORM
# -----------------------------------------------------------------
def build_company_stats(df_safe: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate alumni counts by current company.
    Input df_safe is expected to contain SAFE_COLUMNS (or a subset).
    Output schema: company_name, alumni_count
    """
    df_company = (
        df_safe.reindex(columns=["Current Company"])
        .dropna(subset=["Current Company"])
        .copy()
    )

    stats_company = (
        df_company.groupby("Current Company")
        .size()
        .reset_index(name="alumni_count")
        .rename(columns={"Current Company": "company_name"})
    )

    return stats_company[["company_name", "alumni_count"]]


def build_job_title_stats(df_safe: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate alumni counts by current job title.
    Output schema: job_title, job_count
    """
    df_jobs = (
        df_safe.reindex(columns=["Current Title"])
        .dropna(subset=["Current Title"])
        .copy()
    )

    stats_jobs = (
        df_jobs.groupby("Current Title")
        .size()
        .reset_index(name="job_count")
        .rename(columns={"Current Title": "job_title"})
    )

    return stats_jobs[["job_title", "job_count"]]


def build_major_stats(df_safe: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate alumni counts by major.
    Output schema: major, major_count
    """
    df_major = (
        df_safe.reindex(columns=["Major"])
        .dropna(subset=["Major"])
        .copy()
    )

    stats_major = (
        df_major.groupby("Major")
        .size()
        .reset_index(name="major_count")
        .rename(columns={"Major": "major"})
    )

    return stats_major[["major", "major_count"]]


def build_location_stats(df_safe: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize Location (City, State) and aggregate alumni counts by geo.
    Output schema: country, state, city, alumni_count
    """
    df_location = (
        df_safe.reindex(columns=["Location"])
        .dropna(subset=["Location"])
        .copy()
    )

    # Split "City, State" into two columns; if malformed, state_raw may be NaN
    df_location[["city", "state_raw"]] = df_location["Location"].str.split(
        ",", expand=True, n=1
    )

    df_location["city"] = df_location["city"].str.strip()
    df_location["state"] = df_location["state_raw"].str.strip()
    df_location["country"] = "United States"

    stats_location = (
        df_location.groupby(["country", "state", "city"])
        .size()
        .reset_index(name="alumni_count")
    )

    return stats_location[["country", "state", "city", "alumni_count"]]
