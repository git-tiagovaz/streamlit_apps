import os
import re
import streamlit as st
import pandas as pd
from datetime import date
from google.cloud import bigquery
from google.oauth2 import service_account
import openai
import warnings

warnings.filterwarnings("ignore", message="BigQuery Storage module not found")

# === Load credentials from Streamlit secrets ===
gcp_credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
BQ_PROJECT_ID = st.secrets["gcp_service_account"]["project_id"]
OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
BQ_TABLE = "events_*"

# === Validate required keys ===
if not OPENAI_API_KEY:
    st.error("❌ Missing OpenAI API Key in secrets.")
    st.stop()

# === Init clients ===
openai.api_key = OPENAI_API_KEY
client = bigquery.Client(credentials=gcp_credentials, project=BQ_PROJECT_ID)

# === Brand to Dataset Mapping ===
BRAND_DATASETS = {
    "CH": "analytics_287277614",
    "PEN": "analytics_315842208",
    "LAP": "analytics_396352628",
    "JPG": "analytics_320372229",
    "RABANNE": "analytics_324242252",
    "KAMA": "analytics_338475293",
    "DVN": "analytics_304079916",
}

# === Streamlit Setup ===
st.set_page_config(page_title="Ecommerce Data Assistant", layout="wide")
st.title("🧠 Ecommerce Data Assistant with Memory")

# === Session State ===
if "messages" not in st.session_state:
    st.session_state.messages = []
if "has_started_chat" not in st.session_state:
    st.session_state.has_started_chat = False

# === Welcome Message ===
if not st.session_state.has_started_chat:
    st.markdown("""
    ### 🧭 Welcome to Your Ecommerce Data Assistant

    💬 Just ask your question in plain English — no SQL needed!

    You can ask things like:
    - “How many purchases did we have last week?”
    - “Compare this month’s revenue to the last one”
    - “Show top 5 countries by users in the last 30 days”

    📊 Your assistant will:
    1. Understand your question  
    2. Query live GA4 BigQuery data  
    3. Show results with summaries and optional charts

    👉 Select a brand from the sidebar to get started. Then type your question below — we’ll handle the rest.
    """)
    st.session_state.has_started_chat = True
    st.markdown("---")

# === Brand Selection Sidebar ===
st.sidebar.header("🛍️ Select Brand")
selected_brand = st.sidebar.selectbox("Choose a brand:", list(BRAND_DATASETS.keys()))
selected_dataset = BRAND_DATASETS[selected_brand]
st.sidebar.success(f"Using dataset: `{selected_dataset}`")

# === Reset Chat Button ===
if st.sidebar.button("🔄 Reset Chat"):
    st.session_state.messages = []
    st.sidebar.success("Chat history cleared.")
    st.rerun()

# === Show chat history ===
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# === GPT Helpers ===

def clean_sql_output(raw_sql):
    return re.sub(r"```sql|```", "", raw_sql).strip()

def summarize_dataframe(df: pd.DataFrame, user_question: str) -> str:
    sample_data = df.head(50).to_csv(index=False)

    prompt = f"""
You are a data analyst assistant. A user asked: "{user_question}".

Here is the BigQuery data result:

{sample_data}

Please summarize the main findings and provide key insights in a clear and concise way. Only return your answer in plain English, no code or markdown.
"""

    response = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.5
    )
    return response.choices[0].message.content.strip()

def generate_sql_from_question_with_memory(history, latest_question, selected_dataset):
    today_str = date.today().strftime('%Y-%m-%d')

    prompt = f"""
Act like a Senior GA4 data analyst with extensive knowledge on querying GA4 raw data and analysing GA4 data results with great insights and recommendations, taking in consideration seasonality, trends, campaign and traffic performance, checkout funnel analysis, etc. Your job is to write SQL queries using the GA4 BigQuery dataset.\
Use the latest queries and best practices to answer the questions from the users, without exceeding the limits of BigQuery.\
The GA4 BigQuery export table is called `{BQ_PROJECT_ID}.{selected_dataset}.{BQ_TABLE}` and includes ecommerce events like 'purchase', 'session_start', etc.

Today’s date is {today_str}. When the user mentions relative dates like "last 7 days", "yesterday", "this month", "last year", etc., \
or asks to compare two time periods, calculate the appropriate date ranges accordingly to {today_str} and return a comparison breakdown (e.g. current vs previous).

Use the `PARSE_DATE('%Y%m%d', event_date) AS event_date` inside the SELECT clause and NOT on the WHERE clause. This of course is only needed when the users asks questions about relative dates. \
Use the _TABLE_SUFFIX BETWEEN 'YYYYMMDD' AND 'YYYYMMDD'` in the WHERE clause whenever you're using relative dates. \
For brands datasets selected as Rabanne and JPG, please remember to ALWAYS exclude the hostname that contains fashion. You can use the device.web_info.hostname where the word fashion appears. We don't want to include this hostname in our analysis.
For questions on products or items please always make reference to the items RECORD in GA4 bigquery schema contains information about items included in an event. Don't forget to use the unnest to grab the item name or revenue. It is repeated for each item.
For questions on revenue, use `ecommerce.purchase_revenue`.

Here is a sample question: "{latest_question}"

Generate a valid BigQuery Standard SQL query to answer the question.
Do not include any explanation, comments, or SQL code block formatting. In case the user asks for something outside of your scope, answer that you are not able to help with that.
"""

    messages = history + [{"role": "user", "content": prompt}]
    response = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        temperature=0
    )
    return clean_sql_output(response.choices[0].message.content)

# === BigQuery Runner ===
def run_query(sql):
    query_job = client.query(sql)
    return query_job.result().to_dataframe()

# === Chat Input Handler ===
user_prompt = st.chat_input("Ask a question about your ecommerce data...")

if user_prompt:
    st.chat_message("user").markdown(user_prompt)
    st.session_state.messages.append({"role": "user", "content": user_prompt})

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                raw_sql = generate_sql_from_question_with_memory(
                    st.session_state.messages, user_prompt, selected_dataset
                )
                st.code(raw_sql, language="sql")

                df = run_query(raw_sql)
                st.success("✅ Query ran successfully!")
                st.dataframe(df)

                with st.spinner("🧠 Generating insights..."):
                    summary = summarize_dataframe(df, user_prompt)
                    st.markdown("### 📊 Insight Summary")
                    st.info(summary)

                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"Here is the result of your query:\n```sql\n{raw_sql}\n```\n\n**Insight Summary:**\n{summary}"
                })

                if st.checkbox("📊 Show chart (if numeric/time-based)?"):
                    st.line_chart(df.select_dtypes(include='number'))

            except Exception as e:
                st.error(f"❌ Error executing query: {e}")
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"❌ Error executing query: {e}"
                })
