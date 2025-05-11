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
    "CH": {"dataset": "analytics_287277614", "schema": "GA4"},
    "PEN": {"dataset": "analytics_315842208", "schema": "GA4"},
    "LAP NEW SITE": {"dataset": "analytics_396352628", "schema": "GA4"},
    "JPG": {"dataset": "analytics_320372229", "schema": "GA4"},
    "RABANNE": {"dataset": "analytics_324242252", "schema": "GA4"},
    "KAMA": {"dataset": "analytics_338475293", "schema": "GA4"},
    "DVN": {"dataset": "analytics_304079916", "schema": "GA4"},
    "LAP ISHOP GA4": {"dataset": "analytics_432002833", "schema": "GA4"},
    "LAP GAU: UK + US": {"dataset": "76324491", "schema": "UA"},
    "LAP GAU: FR": {"dataset": "76330830", "schema": "UA"},
}

# === Streamlit Setup ===
st.set_page_config(page_title="Ecommerce Data Assistant", layout="wide")
st.title("🧐 Ecommerce Data Assistant with Memory")

# === Session State ===
if "messages" not in st.session_state:
    st.session_state.messages = []
if "has_started_chat" not in st.session_state:
    st.session_state.has_started_chat = False

# === Brand Selection Sidebar ===
st.sidebar.markdown("Choose a brand to analyze.")
selected_brand = st.sidebar.selectbox("Choose a brand:", list(BRAND_DATASETS.keys()))
selected_config = BRAND_DATASETS[selected_brand]
selected_dataset = selected_config["dataset"]
schema_type = selected_config["schema"]
st.sidebar.success(f"Using: {selected_dataset} ({schema_type.upper()})")

# === Reset Chat Button ===
if st.sidebar.button("🔄 Reset Chat"):
    st.session_state.messages = []
    st.sidebar.success("Chat history cleared.")
    st.rerun()


# === Welcome Message ===
if not st.session_state.has_started_chat:
    st.markdown("""
    ### 🗭 Welcome to Your Ecommerce Data Assistant

    💬 Just ask your question in plain English — no SQL needed!

    You can ask things like:
    - “How many purchases did we have last week?”
    - “Compare this month’s revenue to the last one”
    - “Show top 5 countries by users in the last 30 days”

    📊 Your assistant will:
    1. Understand your question  
    2. Query live GA4 BigQuery data  
    3. Show results with summaries and optional charts

    👈 Select a brand from the sidebar to get started. Then type your question below — we’ll handle the rest.
    """)
    st.session_state.has_started_chat = True
    st.markdown("---")


# === Show chat history ===
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# === GPT Helpers ===

def clean_sql_output(raw_sql):
    return re.sub(r"sql|", "", raw_sql).strip()

def summarize_dataframe(df: pd.DataFrame, user_question: str) -> str:
    sample_data = df.head(50).to_csv(index=False)

    prompt = f"""
You are a senior Google Analytics 4 (GA4) data analyst reviewing raw BigQuery data extracted from GA4 for ecommerce and website performance. Your task is to interpret the dataset below and provide a concise, insightful summary in plain English.

A user asked the following question:
\"\"\"{user_question}\"\"\"

Below is a sample of the query result (first 50 rows):
{sample_data}

Based on the data above, perform the following:
- Identify key patterns, trends, or anomalies relevant to the user question.
- Mention top metrics (e.g., revenue, purchases, sessions, users) only if present.
- Reference temporal changes (e.g., growth, decline, stability) if dates are part of the data.
- Use non-technical language where possible, as if explaining to a digital marketing manager.
- Focus on business impact: what the data means and what could be actionable.

Do **not** return code, markdown, or SQL — only write a plain English paragraph with clear, practical insights.
"""
    response = openai.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.5
    )
    return response.choices[0].message.content.strip()

def load_prompt(template_path, **kwargs):
    with open(template_path, "r") as f:
        template = f.read()
    return template.format(**kwargs)

def generate_sql_from_question_with_memory(history, latest_question, selected_dataset, schema_type):
    today_str = date.today().strftime('%Y-%m-%d')
    prompt_template = "ga4_sql_prompt.txt" if schema_type.lower() == "ga4" else "ua_sql_prompt.txt"
    prompt = load_prompt(
        prompt_template,
        BQ_PROJECT_ID=BQ_PROJECT_ID,
        selected_dataset=selected_dataset,
        BQ_TABLE=BQ_TABLE,
        today_str=today_str,
        latest_question=latest_question
    )
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
                    st.session_state.messages, user_prompt, selected_dataset, schema_type
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
                    "content": f"Here is the result of your query:\n sql\n{raw_sql}\n\n{summary}"
                })

                if st.checkbox("📊 Show chart (if numeric/time-based)?"):
                    st.line_chart(df.select_dtypes(include='number'))

            except Exception as e:
                st.error(f"❌ Error executing query: {e}")
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"❌ Error executing query: {e}"
                })