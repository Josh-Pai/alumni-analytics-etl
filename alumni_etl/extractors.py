import pandas as pd

# -----------------------------------------------------------------
# EXTRACTORS
# -----------------------------------------------------------------

def extract_from_csv(csv_path: str) -> pd.DataFrame:
    """
    Extract raw records from a CSV file (demo / local mode).
    """
    print(f"Starting: Loading demo data from CSV: {csv_path}")
    df_raw = pd.read_csv(csv_path)
    print(f"Successfully loaded {len(df_raw)} raw records from CSV.")
    return df_raw

def extract_from_airtable(airtable_client) -> pd.DataFrame:
    """
    Extract raw records from Airtable and return a DataFrame of fields.
    """
    print("Starting: Fetching all records from Airtable...")
    all_records = airtable_client.get_all()
    df_raw = pd.DataFrame([r.get('fields', {}) for r in all_records])
    print(f"Successfully extracted {len(df_raw)} raw records.")
    return df_raw