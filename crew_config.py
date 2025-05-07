from crewai import Task, Crew
from agents.interpreter import interpreter_agent
from agents.sql_generator import sql_generator_agent
from agents.query_runner import query_runner_agent
from agents.summarizer import summarizer_agent
from tools.bq_tool import BigQueryTool
import openai
import datetime

# Load prompt template
def load_prompt(path, **kwargs):
    with open(path, 'r') as f:
        return f.read().format(**kwargs)

# Main Crew runner
def run_crew(question, selected_dataset, bq_table, project_id):
    today = datetime.date.today().strftime('%Y-%m-%d')
    prompt = load_prompt(
        "prompts/ga4_sql_prompt.txt",
        BQ_PROJECT_ID=project_id,
        selected_dataset=selected_dataset,
        BQ_TABLE=bq_table,
        today_str=today,
        latest_question=question
    )

    sql_tool = BigQueryTool()

    task1 = Task(
        agent=interpreter_agent,
        description="Understand the user question and determine intent.",
        expected_output="Structured ecommerce question details."
    )

    task2 = Task(
        agent=sql_generator_agent,
        description="Generate BigQuery SQL using GA4 dataset from user query.",
        expected_output="GA4-compatible SQL statement.",
        context=prompt
    )

    task3 = Task(
        agent=query_runner_agent,
        description="Execute the SQL and return the resulting DataFrame.",
        expected_output="Raw query result in a DataFrame.",
        tools=[sql_tool]
    )

    task4 = Task(
        agent=summarizer_agent,
        description="Summarize the DataFrame into a business insight.",
        expected_output="Plain English insight about ecommerce performance."
    )

    crew = Crew(
        agents=[interpreter_agent, sql_generator_agent, query_runner_agent, summarizer_agent],
        tasks=[task1, task2, task3, task4],
        verbose=True
    )

    result = crew.run()
    return result
