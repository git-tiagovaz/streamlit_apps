from google.cloud import bigquery

def run_query(client, sql):
    """Run a BigQuery query and return results as a DataFrame."""
    query_job = client.query(sql)
    return query_job.result().to_dataframe()

def dry_run_query(client, sql):
    """Dry run to estimate bytes processed."""
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    query_job = client.query(sql, job_config=job_config)
    return query_job.total_bytes_processed
