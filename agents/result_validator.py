# streamlit_apps/chatbot_app/agents/result_validator.py

from google.cloud import bigquery
import pandas as pd
import os

# Load env for fallback (if not using Streamlit secrets)
from dotenv import load_dotenv
load_dotenv()

# === Initialize BigQuery client ===
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_SERVICE_ACCOUNT_JSON = os.getenv("BQ_SERVICE_ACCOUNT_JSON")

if BQ_SERVICE_ACCOUNT_JSON:
    client = bigquery.Client.from_service_account_json(BQ_SERVICE_ACCOUNT_JSON)
else:
    client = bigquery.Client(project=BQ_PROJECT_ID)

# === Query Validator Agent ===
def validate_query_safety(sql: str, quota_limit_gb: float = 2.0) -> dict:
    """
    Uses dry run to check query size and returns:
    - estimated bytes processed
    - is_safe: True if under quota
    """
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

    try:
        query_job = client.query(sql, job_config=job_config)
        bytes_processed = query_job.total_bytes_processed
        gb_processed = bytes_processed / (1024**3)

        return {
            "bytes_processed": bytes_processed,
            "gb_processed": gb_processed,
            "is_safe": gb_processed < quota_limit_gb
        }
    except Exception as e:
        return {
            "error": str(e),
            "is_safe": False
        }

def run_query_and_check_empty(sql: str) -> dict:
    """
    Executes the SQL and checks if result is empty.
    Returns result and empty status.
    """
    try:
        df = client.query(sql).result().to_dataframe()
        return {
            "is_empty": df.empty,
            "result": df,
            "error": None
        }
    except Exception as e:
        return {
            "is_empty": True,
            "result": pd.DataFrame(),
            "error": str(e)
        }
