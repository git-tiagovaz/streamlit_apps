from langchain.agents import create_react_agent, AgentExecutor
from langchain.chat_models import ChatOpenAI
from langchain.tools import Tool
from langchain.prompts import PromptTemplate
import pandas as pd

def get_chart_recommendation_tool():
    def recommend_chart_from_df(df_str: str) -> str:
        return f"""Here is a DataFrame sample:\n{df_str}\n\nBased on this data, recommend the most appropriate chart type."""

    tool = Tool(
        name="Chart Recommender",
        func=recommend_chart_from_df,
        description="Analyzes a DataFrame sample and recommends a chart type (e.g., line, bar, funnel, pie, etc.)"
    )
    return tool

def create_chart_agent():
    tool = get_chart_recommendation_tool()
    llm = ChatOpenAI(temperature=0.2, model="gpt-4.1")
    
    prompt = PromptTemplate.from_template("""
You are a data visualization expert with years of experience in the field of google analytics data analysis. Your job is to look at the sample structure of a DataFrame (column names and data examples) and recommend a chart type.

Respond ONLY with one of the following: line, bar, pie, funnel, table, scatter plot, etc.

For example:
- If the data has 'date' or 'event_date', suggest 'line'
- If the data has categories like 'country', 'device', 'channel', suggest 'bar'
- If the data looks like steps in a funnel, suggest 'funnel'
- If the data is too wide or not chartable, suggest 'table'

Data sample:
{input}
""")

    agent = create_react_agent(llm=llm, tools=[tool], prompt=prompt)
    return AgentExecutor(agent=agent, tools=[tool], verbose=True)
