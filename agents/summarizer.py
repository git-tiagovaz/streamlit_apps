from crewai import Agent

summarizer_agent = Agent(
    role="Insight Summarizer",
    goal="Summarize query results into business insights",
    backstory="Experienced in interpreting ecommerce analytics results and translating them into plain English insights.",
    verbose=True
)
