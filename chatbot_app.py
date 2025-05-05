import os
import re
import streamlit as st
import pandas as pd
from datetime import date
from google.cloud import bigquery
from google.oauth2 import service_account
import openai
import warnings
from dotenv import load_dotenv

# === Agent imports ===
from agents.sql_generator import generate_sql
from agents.quota_checker import exceeds_quota
from agents.result_validator import validate_query_safety, run_query_and_check_empty
from agents.chart_recommender import recommend_chart_type
from agents.insight_expander import suggest_additional_insights

warnings.filterwarnings("ignore", message="BigQuery Storage module not found")
load_dotenv()

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
st.title("🧠 Ecommerce Data Assistant with Multi-Agent System")

# === Session State ===
if "messages" not in st.session_state:
    st.session_state.messages = []
if "has_started_chat" not in st.session_state:
    st.session_state.has_started_chat = False

# === Welcome Message ===
if not st.session_state.has_started_chat:
    st.markdown("""
    ### 🧭 Welcome to Your Multi-Agent GA4 Ecommerce Assistant

    Ask anything about purchases, conversion funnels, revenue trends, or user behavior.

    👈 Select a brand from the sidebar and begin!
    """)
    st.session_state.has_started_chat = True
    st.markdown("---")

# === Brand Selection Sidebar ===
st.sidebar.header("🏭 Select Brand")
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

# === Utility: Load SQL Prompt Template ===
def load_prompt(template_path, **kwargs):
    with open(template_path, "r") as f:
        template = f.read()
    return template.format(**kwargs)

# === BigQuery Runner ===
def run_query(sql):
    query_job = client.query(sql)
    return query_job.result().to_dataframe()

# === Insight Summary ===
def summarize_dataframe(df: pd.DataFrame, user_question: str) -> str:
    sample_data = df.head(50).to_csv(index=False)
    prompt = f"""
You are a senior GA4 data analyst. A user asked: \"{user_question}\"

Below is a preview of the query result:
{sample_data}

Summarize the findings using plain English. Mention any patterns, drops, spikes, or trends. Keep it actionable and readable by non-technical stakeholders.
"""
    response = openai.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.5
    )
    return response.choices[0].message.content.strip()

# === Chat Input Handler ===
user_prompt = st.chat_input("Ask a question about your ecommerce data...")

if user_prompt:
    st.chat_message("user").markdown(user_prompt)
    st.session_state.messages.append({"role": "user", "content": user_prompt})

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                # === SQL Generation ===
                today_str = date.today().strftime('%Y-%m-%d')
                sql_prompt = load_prompt(
                    "ga4_sql_prompt.txt",
                    BQ_PROJECT_ID=BQ_PROJECT_ID,
                    selected_dataset=selected_dataset,
                    BQ_TABLE=BQ_TABLE,
                    today_str=today_str,
                    latest_question=user_prompt
                )
                sql_query = generate_sql(sql_prompt)
                st.code(sql_query, language="sql")

                # === Cost Check ===
                if not exceeds_quota(sql_query, client):
                    st.warning("⚠️ This query may exceed BigQuery quota limits (2GB scanned). Please refine your question.")
                    
                # === Run Query ===
                df = run_query_and_check_empty(sql_query)
                if df.empty(df):
                    st.warning("⚠️ Query returned no results.")

                st.success("✅ Query ran successfully!")
                st.dataframe(df)

                # === Insight Summary ===
                with st.spinner("🧠 Generating insights..."):
                    summary = summarize_dataframe(df, user_prompt)
                    st.markdown("### 📊 Insight Summary")
                    st.info(summary)

                # === Chart Recommendation ===
                recommended_chart = recommend_chart_type(df, user_prompt)
                if recommended_chart:
                    st.markdown(f"### 📈 Recommended Chart: `{recommended_chart}`")
                    if recommended_chart == "line":
                        st.line_chart(df.select_dtypes(include='number'))
                    elif recommended_chart == "bar":
                        st.bar_chart(df.select_dtypes(include='number'))

                # === Insight Suggestions ===
                suggestions = suggest_additional_insights(user_prompt, df.head(20).to_csv(index=False))
                st.markdown("### 🔍 You might also ask:")
                st.markdown(suggestions)

                # === Save history ===
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"Here is the result of your query:\n```sql\n{sql_query}\n```\n\n**Insight Summary:**\n{summary}\n\n**Suggestions:**\n{suggestions}"
                })

            except Exception as e:
                st.error(f"❌ Error: {e}")
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"❌ Error executing query: {e}"
                })
