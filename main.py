import os
import pandas as pd
from dotenv import load_dotenv
from airtable import Airtable
from google.cloud import bigquery
from alumni_etl.extractors import extract_from_airtable, extract_from_csv
from alumni_etl.loaders import load_stats_tables


# -----------------------------------------------------------------
# SETUP
# -----------------------------------------------------------------

# Load environment variables from .env file
print("Loading environment variables...")
load_dotenv()

# Columns allowed for processing in transformations
SAFE_COLUMNS = [
    'Current Company',
    'Current Title',
    'Location',
    'Major',
    'Graduation Year'
]

# Runtime mode controls how THIS run behaves (no code changes needed)
# - DATA_SOURCE: where raw data comes from
# - DATA_MODE: where aggregated outputs are written
DATA_SOURCE = os.getenv("DATA_SOURCE", "airtable").strip().lower()   # airtable | csv
DATA_MODE = os.getenv("DATA_MODE", "prod").strip().lower()           # prod | demo


# -----------------------------------------------------------------
# Prod Config
# -----------------------------------------------------------------

# Source - Airtable (only required when DATA_SOURCE=airtable)
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
AIRTABLE_TABLE_NAME = os.getenv('AIRTABLE_TABLE_NAME')
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')

# Destination - BigQuery (project is always required; dataset depends on DATA_MODE)
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BIGQUERY_DATASET_ID = os.getenv('BIGQUERY_DATASET_ID')  # prod dataset id
# GOOGLE_APPLICATION_CREDENTIALS is read automatically by the Google client library

# -----------------------------------------------------------------
# Demo Config
# -----------------------------------------------------------------

# Source - CSV (used when DATA_SOURCE=csv)
DEMO_CSV_PATH = os.getenv("DEMO_CSV_PATH", "data/demo_alumni.csv")

# Destination - BigQuery demo dataset (required when DATA_MODE=demo)
BIGQUERY_DEMO_DATASET_ID = os.getenv("BIGQUERY_DEMO_DATASET_ID")  # demo dataset id

# -----------------------------------------------------------------
# Validate runtime configuration early
# -----------------------------------------------------------------
if DATA_SOURCE not in {"airtable", "csv"}:
    raise ValueError("Invalid DATA_SOURCE. Use 'airtable' or 'csv'.")

if DATA_MODE not in {"prod", "demo"}:
    raise ValueError("Invalid DATA_MODE. Use 'prod' or 'demo'.")

if not GCP_PROJECT_ID:
    raise ValueError("Missing GCP_PROJECT_ID (required).")

if DATA_MODE == "prod" and not BIGQUERY_DATASET_ID:
    raise ValueError("Missing BIGQUERY_DATASET_ID (required when DATA_MODE=prod).")

if DATA_MODE == "demo" and not BIGQUERY_DEMO_DATASET_ID:
    raise ValueError("Missing BIGQUERY_DEMO_DATASET_ID (required when DATA_MODE=demo).")

if DATA_SOURCE == "airtable":
    missing = [k for k, v in {
        "AIRTABLE_BASE_ID": AIRTABLE_BASE_ID,
        "AIRTABLE_TABLE_NAME": AIRTABLE_TABLE_NAME,
        "AIRTABLE_API_KEY": AIRTABLE_API_KEY,
    }.items() if not v]
    if missing:
        raise ValueError(f"Missing Airtable config: {', '.join(missing)} (required when DATA_SOURCE=airtable).")


# -----------------------------------------------------------------
# Initialize Clients
# -----------------------------------------------------------------
# Create external clients based on runtime mode.
# Airtable client is only required when DATA_SOURCE=airtable.
# BigQuery client is required for loading outputs.

airtable = None  # only initialized in airtable mode

try:
    if DATA_SOURCE == "airtable":
        print("Connecting to Airtable...")
        airtable = Airtable(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME, api_key=AIRTABLE_API_KEY)

    print("Connecting to BigQuery...")
    bigquery_client = bigquery.Client()

    print("Connections initialized.")
except Exception as e:
    print(f"❌ ERROR: Failed to initialize clients. {e}")
    exit(1)



def get_target_dataset_id() -> str:
    """
    Select the BigQuery dataset to write to based on DATA_MODE.
    - prod: BIGQUERY_DATASET_ID
    - demo: BIGQUERY_DEMO_DATASET_ID
    """
    return BIGQUERY_DEMO_DATASET_ID if DATA_MODE == "demo" else BIGQUERY_DATASET_ID



# -----------------------------------------------------------------
# TRANSFORM - Pure Functions
# -----------------------------------------------------------------
def build_company_stats(df_safe):
    # As discovered in the prototype, we must drop rows where the company is null
    df_company = df_safe.reindex(columns=['Current Company']).dropna(subset=['Current Company']).copy()
    stats_company = df_company.groupby('Current Company').size().reset_index(name='alumni_count')
    stats_company = stats_company.rename(columns={'Current Company': 'company_name'})
    stats_company = stats_company[['company_name', 'alumni_count']]
    return stats_company


def build_job_title_stats(df_safe):
    df_jobs = df_safe.reindex(columns=['Current Title']).dropna(subset=['Current Title']).copy()
    stats_jobs = df_jobs.groupby('Current Title').size().reset_index(name='job_count')
    stats_jobs = stats_jobs.rename(columns={'Current Title': 'job_title'})
    stats_jobs = stats_jobs[['job_title', 'job_count']]
    return stats_jobs


def build_major_stats(df_safe):
    df_major = df_safe.reindex(columns=['Major']).dropna(subset=['Major']).copy()
    stats_major = df_major.groupby('Major').size().reset_index(name='major_count')
    stats_major = stats_major.rename(columns={'Major': 'major'})
    stats_major = stats_major[['major', 'major_count']]
    return stats_major


def build_location_stats(df_safe):
    df_location = df_safe.reindex(columns=['Location']).dropna(subset=['Location']).copy()

    # Split "City, State" into two columns
    df_location[['city', 'state_raw']] = df_location['Location'].str.split(',', expand=True, n=1)

    # Clean whitespace and add Country context
    df_location['city'] = df_location['city'].str.strip()
    df_location['state'] = df_location['state_raw'].str.strip()
    df_location['country'] = 'United States'

    # Group by the new, clean columns
    stats_location = df_location.groupby(['country', 'state', 'city']).size().reset_index(name='alumni_count')
    stats_location = stats_location[['country', 'state', 'city', 'alumni_count']]
    return stats_location


# -----------------------------------------------------------------
# MAIN ETL FUNCTION
# -----------------------------------------------------------------
def run_etl():
    """Main ETL (Extract, Transform, Load) pipeline."""

    # -----------------------------------------------------------------
    # EXTRACT
    # -----------------------------------------------------------------
    print("\n--- EXTRACT ---")
    try:
        if DATA_SOURCE == "csv":
            # Demo/local mode: read from CSV
            df_raw = extract_from_csv(DEMO_CSV_PATH)
        else:
            # Production mode: read from Airtable
            df_raw = extract_from_airtable(airtable)
    except Exception as e:
        print("❌ ERROR: Failed to extract data.")
        print(f"   └── Details: {e}")
        return



    # -----------------------------------------------------------------
    # TRANSFORM
    # -----------------------------------------------------------------
    print("\n--- TRANSFORM ---")
    print("Starting: Anonymizing and aggregating data...")

    # Use reindex for robust schema definition
    df_safe = df_raw.reindex(columns=SAFE_COLUMNS)

    # --- T1: Company Stats ---
    stats_company = build_company_stats(df_safe)
    print(f"  Processed {len(stats_company)} Company aggregates.")

    # --- T2: Job Title Stats ---
    stats_jobs = build_job_title_stats(df_safe)
    print(f"  Processed {len(stats_jobs)} Job Title aggregates.")

    # --- T3: Major Stats ---
    stats_major = build_major_stats(df_safe)
    print(f"  Processed {len(stats_major)} Major aggregates.")

    # --- T4: Location Stats (Geospatial Normalization) ---
    stats_location = build_location_stats(df_safe)
    print(f"  Processed {len(stats_location)} Location aggregates.")

    print("Transformation complete.")

    # -----------------------------------------------------------------
    # LOAD
    # -----------------------------------------------------------------
    print("\n--- LOAD ---")

    # Choose target dataset based on runtime mode
    target_dataset_id = get_target_dataset_id()

    outputs = {
        "stats_company": stats_company,
        "stats_job_title": stats_jobs,
        "stats_major": stats_major,
        "stats_location": stats_location,
    }

    load_stats_tables(
        bigquery_client=bigquery_client,
        project_id=GCP_PROJECT_ID,
        dataset_id=target_dataset_id,
        outputs=outputs,
    )


    print("\nETL pipeline finished successfully!")

# -----------------------------------------------------------------
# PYTHON ENTRY POINT
# -----------------------------------------------------------------
if __name__ == "__main__":
    run_etl()
