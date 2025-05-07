import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st

class BigQueryTool:
    def __init__(self):
        self.client = bigquery.Client(
            credentials=service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            ),
            project=st.secrets["gcp_service_account"]["project_id"]
        )

    def run_query(self, sql: str) -> pd.DataFrame:
        return self.client.query(sql).result().to_dataframe()
