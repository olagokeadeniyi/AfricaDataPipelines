# shared/storage.py
import os
from google.cloud import storage

# Set GOOGLE_APPLICATION_CREDENTIALS local dev: export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"
def _get_client():
    return storage.Client()

def upload_raw(bucket_name, path, content):
    """
    bucket_name: either 'africadata-raw' (GCS bucket) or full URI 'gs://africadata-raw'
    path: path inside bucket, e.g., 'worldbank/SP.POP.TOTL/..json'
    content: bytes or str
    """
    # normalize bucket name
    if bucket_name.startswith("gs://"):
        bucket_name = bucket_name.replace("gs://", "")
    client = _get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(path)
    if isinstance(content, bytes):
        blob.upload_from_string(content)
    else:
        blob.upload_from_string(content, content_type='application/json')
    # optional metadata:
    blob.patch()
    return f"gs://{bucket_name}/{path}"
