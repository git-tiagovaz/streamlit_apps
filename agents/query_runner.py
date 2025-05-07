from crewai import Agent

query_runner_agent = Agent(
    role="Query Executor",
    goal="Run SQL queries against BigQuery and return results",
    backstory="Expert at executing SQL securely and efficiently using Google BigQuery.",
    verbose=True
)
