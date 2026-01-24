import pandas as pd
from alumni_etl.transforms import build_location_stats


def test_build_location_stats_strips_whitespace_and_aggregates():
    # Arrange: two semantically identical locations with different whitespace, plus a distinct location
    df = pd.DataFrame({
        "Location": [
            "Cincinnati, OH",
            " Cincinnati , OH ",
            "Columbus, OH",
            None,  # should be dropped by dropna
        ]
    })

    # Act: run location normalization + aggregation
    result = build_location_stats(df)

    # Assert: output schema is stable
    assert list(result.columns) == ["country", "state", "city", "alumni_count"]

    # Assert: Cincinnati rows are normalized and aggregated into a single group with count=2
    cincy = result[(result["city"] == "Cincinnati") & (result["state"] == "OH")]
    assert len(cincy) == 1
    assert int(cincy.iloc[0]["alumni_count"]) == 2

    # Assert: Columbus appears once
    columbus = result[(result["city"] == "Columbus") & (result["state"] == "OH")]
    assert len(columbus) == 1
    assert int(columbus.iloc[0]["alumni_count"]) == 1
