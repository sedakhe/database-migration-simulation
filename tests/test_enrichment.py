import pandas as pd

def test_country_enrichment():
    users = pd.DataFrame([{"user_id": 1, "country_code": "IN"}])
    dim = pd.DataFrame([
        {"country_code": "IN", "country_name": "India"},
        {"country_code": "US", "country_name": "United States"},
    ])
    enriched = users.merge(dim, on="country_code", how="left")
    assert enriched.iloc[0]["country_name"] == "India"
