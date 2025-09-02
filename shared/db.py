# shared/db.py
import os
import pandas as pd
from google.cloud import bigquery

def to_bigquery(df: pd.DataFrame, project_id: str, dataset: str, table: str, if_exists='replace'):
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # wait
    print(f"Loaded {len(df)} rows to {project_id}.{dataset}.{table}")
    return True
