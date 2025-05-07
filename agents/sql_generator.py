from crewai import Agent

sql_generator_agent = Agent(
    role="SQL Generator",
    goal="Generate GA4 BigQuery SQL from user-friendly questions",
    backstory="Experienced in GA4 schema and BigQuery SQL generation for ecommerce performance.",
    verbose=True
)
