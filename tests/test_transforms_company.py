import pandas as pd
from alumni_etl.transforms import build_company_stats


def test_build_company_stats_basic():
    # Arrange: minimal, representative input covering duplicates and nulls
    df = pd.DataFrame({
        "Current Company": ["Google", "Google", "Amazon", None]
    })

    # Act: run aggregation logic
    result = build_company_stats(df)

    # Assert: null values are excluded and counts are aggregated correctly
    assert len(result) == 2

    google_row = result[result["company_name"] == "Google"].iloc[0]
    amazon_row = result[result["company_name"] == "Amazon"].iloc[0]

    assert google_row["alumni_count"] == 2
    assert amazon_row["alumni_count"] == 1
