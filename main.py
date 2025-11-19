import os
import pandas as pd
from dotenv import load_dotenv
from airtable import Airtable
from google.cloud import bigquery
from google.api_core import exceptions as google_exceptions

# -----------------------------------------------------------------
# SETUP
# -----------------------------------------------------------------

# Load environment variables from .env file
print("Loading environment variables...")
load_dotenv()

# --- Airtable Config ---
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
AIRTABLE_TABLE_NAME = os.getenv('AIRTABLE_TABLE_NAME')
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')

# --- BigQuery Config ---
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BIGQUERY_DATASET_ID = os.getenv('BIGQUERY_DATASET_ID')
# GOOGLE_APPLICATION_CREDENTIALS is read automatically by the Google client library

# --- Initialize Clients ---
# Check if all keys are present before trying to connect
if not all([AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME, AIRTABLE_API_KEY, GCP_PROJECT_ID, BIGQUERY_DATASET_ID]):
    print("❌ ERROR: One or more environment variables are missing from your .env file.")
    print("Please check all 5 variables (AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME, AIRTABLE_API_KEY, GCP_PROJECT_ID, BIGQUERY_DATASET_ID).")
    exit()

try:
    print("Connecting to Airtable...")
    airtable = Airtable(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME, api_key=AIRTABLE_API_KEY)
    
    print("Connecting to BigQuery...")
    bigquery_client = bigquery.Client()
    print("Connections initialized.")
except Exception as e:
    print(f"❌ ERROR: Failed to initialize clients. {e}")
    exit()

# -----------------------------------------------------------------
# LOAD - Helper Function
# -----------------------------------------------------------------
def load_dataframe_to_bigquery(dataframe, table_name):
    """
    Loads a Pandas DataFrame into a specified BigQuery table.
    This function will OVERWRITE the existing table (WRITE_TRUNCATE).
    """
    
    # Full BigQuery path: PROJECT_ID.DATASET_ID.table_name
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    
    try:
        print(f"  Loading {len(dataframe)} rows into {table_id}...")
        # Start the load job
        job = bigquery_client.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        )
        job.result()  # Wait for the job to complete
        print(f"  ✅ SUCCESS: Load complete for {table_id}")
        
    except google_exceptions.NotFound as e:
        print(f"  ❌ ERROR: {table_id} failed to load. The dataset '{BIGQUERY_DATASET_ID}' might not exist.")
        print(f"  └── Details: {e}")
    except Exception as e:
        print(f"  ❌ ERROR: {table_id} failed to load.")
        print(f"  └── Details: {e}")

# -----------------------------------------------------------------
# MAIN ETL FUNCTION
# -----------------------------------------------------------------
def run_etl():
    """Main ETL (Extract, Transform, Load) pipeline."""

    # -----------------------------------------------------------------
    # EXTRACT
    # -----------------------------------------------------------------
    print("\n--- EXTRACT ---")
    print("Starting: Fetching all records from Airtable...")
    try:
        # Get all records from the specified table
        all_records = airtable.get_all()
        # Convert list of field dicts into a raw DataFrame
        df_raw = pd.DataFrame([r['fields'] for r in all_records])
        print(f"Successfully extracted {len(df_raw)} raw records.")
    
    except Exception as e:
        print(f"❌ ERROR: Failed to extract from Airtable. Check your Token, Base ID, and Table Name.")
        print(f"   └── Details: {e}")
        return # Stop the script if extraction fails

    # -----------------------------------------------------------------
    # TRANSFORM
    # -----------------------------------------------------------------
    print("\n--- TRANSFORM ---")
    print("Starting: Anonymizing and aggregating data...")
    
    # Define the columns to use
    safe_columns = [
        'Current Company', 
        'Current Title', 
        'Location', 
        'Major',
        'Graduation Year'
    ]
    
    # Use reindex for robust schema definition
    df_safe = df_raw.reindex(columns=safe_columns)

    # --- T1: Company Stats ---
    # As discovered in the prototype, we must drop rows where the company is null
    df_company = df_safe.dropna(subset=['Current Company']).copy()
    stats_company = df_company.groupby('Current Company').size().reset_index(name='alumni_count')
    stats_company = stats_company.rename(columns={'Current Company': 'company_name'})
    print(f"  Processed {len(stats_company)} Company aggregates.")

    # --- T2: Job Title Stats ---
    df_jobs = df_safe.dropna(subset=['Current Title']).copy()
    stats_jobs = df_jobs.groupby('Current Title').size().reset_index(name='job_count')
    stats_jobs = stats_jobs.rename(columns={'Current Title': 'job_title'})
    print(f"  Processed {len(stats_jobs)} Job Title aggregates.")

    # --- T3: Major Stats ---
    df_major = df_safe.dropna(subset=['Major']).copy()
    stats_major = df_major.groupby('Major').size().reset_index(name='major_count')
    stats_major = stats_major.rename(columns={'Major': 'major'})
    print(f"  Processed {len(stats_major)} Major aggregates.")

    # --- T4: Location Stats (Geospatial Normalization) ---
    df_location = df_safe.dropna(subset=['Location']).copy()
    
    # Split "City, State" into two columns
    df_location[['city', 'state_raw']] = df_location['Location'].str.split(',', expand=True, n=1)
    
    # Clean whitespace and add Country context
    df_location['city'] = df_location['city'].str.strip()
    df_location['state'] = df_location['state_raw'].str.strip()
    df_location['country'] = 'United States'
    
    # Group by the new, clean columns
    stats_location = df_location.groupby(['country', 'state', 'city']).size().reset_index(name='alumni_count')
    print(f"  Processed {len(stats_location)} Location aggregates.")

    print("Transformation complete.")

    # -----------------------------------------------------------------
    # LOAD
    # -----------------------------------------------------------------
    print("\n--- LOAD ---")
    print("Starting: Loading all aggregated tables to BigQuery...")

    # Execute all load operations. The helper function handles WRITE_TRUNCATE.
    load_dataframe_to_bigquery(stats_company, "stats_company")
    load_dataframe_to_bigquery(stats_jobs, "stats_job_title")
    load_dataframe_to_bigquery(stats_major, "stats_major")
    load_dataframe_to_bigquery(stats_location, "stats_location")

    print("\nETL pipeline finished successfully!")

# -----------------------------------------------------------------
# PYTHON ENTRY POINT
# -----------------------------------------------------------------
if __name__ == "__main__":
    run_etl()