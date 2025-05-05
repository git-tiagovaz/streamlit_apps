import re

def clean_sql_output(raw_sql: str) -> str:
    """Clean triple backticks and extra whitespace from raw LLM SQL output."""
    return re.sub(r"```sql|```", "", raw_sql).strip()

def load_prompt(prompt_path: str, **kwargs) -> str:
    """Load a prompt template from a .txt file and format it with keyword arguments."""
    with open(prompt_path, "r") as file:
        template = file.read()
    return template.format(**kwargs)
