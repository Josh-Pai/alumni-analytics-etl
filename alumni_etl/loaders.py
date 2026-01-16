import pandas as pd
from google.cloud import bigquery
from google.api_core import exceptions as google_exceptions

# -----------------------------------------------------------------
# LOADERS
# -----------------------------------------------------------------
def load_dataframe_to_bigquery(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_name: str,
    dataframe: pd.DataFrame,
) -> None:
    """
    Loads a Pandas DataFrame into a specified BigQuery table.
    This function will OVERWRITE the existing table (WRITE_TRUNCATE).
    """
    
    # Full BigQuery path: PROJECT_ID.DATASET_ID.table_name
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    
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
        print(f"  ❌ ERROR: {table_id} failed to load. The dataset '{dataset_id}' might not exist.")
        print(f"  └── Details: {e}")
    except Exception as e:
        print(f"  ❌ ERROR: {table_id} failed to load.")
        print(f"  └── Details: {e}")

def load_stats_tables(
    bigquery_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    outputs: dict[str, pd.DataFrame],
) -> None:
    print("Starting: Loading all aggregated tables to BigQuery...")
    for table_name, df in outputs.items():
        load_dataframe_to_bigquery(
            bigquery_client=bigquery_client,
            project_id=project_id,
            dataset_id=dataset_id,
            table_name=table_name,
            dataframe=df,
        )