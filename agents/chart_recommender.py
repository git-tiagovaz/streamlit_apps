from langchain_core.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from langchain_core.runnables import Runnable

def create_chart_agent() -> Runnable:
    prompt = PromptTemplate.from_template("""
You are a data visualization expert. Based on the following tabular data, suggest the most appropriate chart type to visualize it.
Only reply with one of the following: "timeseries", "bar", "pie", "table", or "stacked bar".

Here is the sample data:

{input}
""")

    chain = prompt | ChatOpenAI(model="gpt-4o", temperature=0.2)
    return chain
