import os
import openai
from datetime import date
from utils.helpers import load_prompt, clean_sql_output

# Load API Key from env or secrets
def configure_openai(api_key):
    openai.api_key = api_key

def generate_sql(history, latest_question, project_id, dataset, table, prompt_path):
    today_str = date.today().strftime('%Y-%m-%d')

    prompt = load_prompt(
        prompt_path,
        BQ_PROJECT_ID=project_id,
        selected_dataset=dataset,
        BQ_TABLE=table,
        today_str=today_str,
        latest_question=latest_question
    )

    messages = history + [{"role": "user", "content": prompt}]

    response = openai.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        temperature=0
    )

    return clean_sql_output(response.choices[0].message.content)
