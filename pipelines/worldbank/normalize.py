# pipelines/worldbank/normalize.py
import json
import pandas as pd
from datetime import datetime

def normalize_worldbank_json(json_data):
    # json_data is the data[1] structure from worldbank
    records = []
    # some endpoints return [meta, data]
    data_records = json_data[1] if isinstance(json_data, list) and len(json_data) > 1 else json_data
    for row in data_records:
        records.append({
            "country_iso3": row.get("countryiso3code"),
            "country": row.get("country", {}).get("value") if row.get("country") else None,
            "indicator_id": row.get("indicator", {}).get("id") if row.get("indicator") else None,
            "indicator": row.get("indicator", {}).get("value") if row.get("indicator") else None,
            "year": int(row.get("date")) if row.get("date") else None,
            "value": row.get("value"),
        })
    df = pd.DataFrame.from_records(records)
    return df

# quick local test runner
if __name__ == "__main__":
    with open("example_worldbank.json") as f:
        j = json.load(f)
    df = normalize_worldbank_json(j)
    print(df.head())
    df.to_parquet("worldbank_population.parquet", index=False)
