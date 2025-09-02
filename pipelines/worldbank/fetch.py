# pipelines/worldbank/fetch.py
import os
import requests
from datetime import datetime

def fetch_indicator(indicator, start=2000, end=2024):
    url = f"https://api.worldbank.org/v2/country/all/indicator/{indicator}"
    params = {"format":"json", "date":f"{start}:{end}", "per_page":10000}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # TODO: upload to cloud storage (shared.storage.upload_raw) in later step
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    with open(f"tmp_{indicator}_{ts}.json", "w", encoding="utf-8") as f:
        import json
        json.dump(data, f)
    print("Saved tmp file for", indicator)

if __name__ == "__main__":
    fetch_indicator("SP.POP.TOTL", 2000, 2024)
