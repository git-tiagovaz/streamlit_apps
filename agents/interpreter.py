from crewai import Agent

interpreter_agent = Agent(
    role="Question Interpreter",
    goal="Understand user ecommerce questions and extract intent",
    backstory="Expert in interpreting ecommerce analytics questions for Google Analytics 4 datasets.",
    verbose=True
)
