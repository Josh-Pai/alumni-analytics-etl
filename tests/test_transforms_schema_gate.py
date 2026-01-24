import pandas as pd
from alumni_etl.transforms import build_company_stats


def test_build_company_stats_missing_required_column_returns_empty():
    # Arrange: input missing "Current Company" entirely
    df = pd.DataFrame({
        "Some Other Column": ["x", "y"]
    })

    # Act: transform should not raise; it should return an empty result with stable schema
    result = build_company_stats(df)

    # Assert: stable output schema and empty rows
    assert list(result.columns) == ["company_name", "alumni_count"]
    assert len(result) == 0
