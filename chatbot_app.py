from vertexai.language_models import ChatModel  # Make sure you import this at the top
import os
import re
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import openai
import warnings
from app_credentials import VALID_USERS
from agents.chart_recommender import create_chart_agent
import vertexai




warnings.filterwarnings("ignore", message="BigQuery Storage module not found")

# === Load credentials from Streamlit secrets ===
gcp_credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
BQ_PROJECT_ID = st.secrets["gcp_service_account"]["project_id"]
OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
BQ_TABLE = "events_*"
CHANNEL_RULES_TABLE = "algebraic-pier-330310.ga4_reference.custom_channel_grouping"

# === Initialize Vertex AI ===
vertexai.init(
    project=st.secrets["gcp_service_account"]["project_id"],
    location="europe-west1",  # Use your preferred region
)


# === Validate required keys ===
if not OPENAI_API_KEY:
    st.error("❌ Missing OpenAI API Key in secrets.")
    st.stop()

# === Init clients ===
openai.api_key = OPENAI_API_KEY
client = bigquery.Client(credentials=gcp_credentials, project=BQ_PROJECT_ID)

def login_ui():
    st.markdown("""
            <style>
        .login-container {
            max-width: 400px;
            margin: auto;
            padding: 2rem;
            background-color: #ffffff;
            border-radius: 10px;
            box-shadow: 0px 4px 10px rgba(0, 0, 0, 0.1);
        }
        .login-title {
            text-align: center;
            font-size: 2em;
            font-weight: 600;
        }
        .login-subtitle {
            text-align: center;
            margin-bottom: 1.5rem;
            font-size: 1.1em;
        }
        .login-icon {
            text-align: center;
            font-size: 3em;
            margin-bottom: 0.5rem;
        }
        </style>
    """, unsafe_allow_html=True)

    with st.container():
        #st.markdown('<div class="login-container">', unsafe_allow_html=True)
        st.image("assets/GA4_Logo.png", width=250)

        st.markdown("### 👤 Login to Ecommerce Data Assistant")

        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        login = st.button("🔐 Login")

        if login:
            if username in VALID_USERS and VALID_USERS[username] == password:
                st.session_state["authenticated"] = True
                st.success(f"Welcome, {username}!")
                st.rerun()
            else:
                st.error("❌ Invalid username or password")
        st.markdown("</div>", unsafe_allow_html=True)


if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if not st.session_state["authenticated"]:
    login_ui()
    st.stop()

def load_markdown(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

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
st.set_page_config(page_title="GA4 Data Assistant", layout="wide")
st.title("GA4 Agentic AI Assistant")
st.markdown("🔍 **Insight Assistant – Not a decision-maker. Use insights to support, not replace, human judgment.**", unsafe_allow_html=True)


# === Session State ===
if "messages" not in st.session_state:
    st.session_state.messages = []
if "has_started_chat" not in st.session_state:
    st.session_state.has_started_chat = False

# === Brand Selection Sidebar ===
st.sidebar.markdown("Brand Selection.")
selected_brand = st.sidebar.selectbox("Choose a brand:", list(BRAND_DATASETS.keys()))
selected_config = BRAND_DATASETS[selected_brand]
selected_dataset = selected_config["dataset"]
schema_type = selected_config["schema"]
st.sidebar.success(f"Using: {selected_dataset} ({schema_type.upper()})")
st.sidebar.markdown("---")

st.sidebar.markdown("## Assistant Mode")
selected_mode = st.sidebar.radio(
    "Choose your assistant:",
    ("GA4 Data Assistant", "Gemini Analytics Assistant")
)
st.sidebar.markdown("---")


# === Sidebar Disclaimer ===
st.sidebar.markdown("---")
st.sidebar.markdown("🔒 ** For internal analytics team use only - no PII data will be revelead or shared **", unsafe_allow_html=True)

# === Welcome Message ===
if not st.session_state.has_started_chat:
    welcome_text = load_markdown("templates/welcome.md")
    st.markdown(welcome_text)
    st.markdown("---")

# === Show chat history ===
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# === GPT Helpers ===

def clean_sql_output(raw_sql):
    return re.sub(r"```sql|```", "", raw_sql).strip()

def summarize_dataframe(df: pd.DataFrame, user_question: str) -> str:
    sample_data = df.head(30).to_csv(index=False)
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

Do **not** return code, markdown, or SQL — only write in plain English.
You can write in bullet points or separated paragraphs for clear, practical insights.
"""
    response = openai.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.5
    )
    return response.choices[0].message.content.strip()

def ask_gemini(question):
    model = ChatModel.from_pretrained("gemini-1.5-pro")  # Use 2.5 when available
    chat = model.start_chat()
    prompt = f"""
You are a senior digital analytics consultant.
Explain the following in simple and strategic language:

{question}
"""
    return chat.send_message(prompt).text

def load_prompt(template_path, **kwargs):
    with open(template_path, "r") as f:
        template = f.read()
    return template.format(**kwargs)

def generate_sql_prompt(history, user_question, selected_dataset, schema_type=None):
    today = date.today() - timedelta(days=2)
    today_str = today.strftime('%Y-%m-%d')  # This becomes your {today_str} in the prompt
    prompt_template = "ga4_sql_prompt.txt"

    # Check if user mentioned channels/acquisition
    if any(kw in user_question.lower() for kw in ["channel", "acquisition", "source", "medium"]):
        channel_join = f"""
    \n\nThe user has asked a channel-based question.
    To enhance the analysis, consider LEFT JOINing with `{CHANNEL_RULES_TABLE}` using `REGEXP_CONTAINS` rules on fields such as `traffic_source.source` and `traffic_source.medium`.
    """
    else:
        channel_join = ""  # No need to mention it in non-channel-related prompts


    prompt = load_prompt(
        prompt_template,
        BQ_PROJECT_ID=BQ_PROJECT_ID,
        selected_dataset=selected_dataset,
        BQ_TABLE=BQ_TABLE,
        today_str=today_str,
        latest_question=user_question
    ) + channel_join

    messages = history + [{"role": "user", "content": prompt}]
    response = openai.chat.completions.create(
        model="gpt-4.1",
        messages=messages,
        temperature=0.2
    )
    return clean_sql_output(response.choices[0].message.content)

# === Query Estimator ===
def estimate_query_size(sql):
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    query_job = client.query(sql, job_config=job_config)
    total_bytes = query_job.total_bytes_processed
    if total_bytes < 1024 ** 2:
        return f"{total_bytes / 1024:.2f} KB"
    elif total_bytes < 1024 ** 3:
        return f"{total_bytes / 1024 ** 2:.2f} MB"
    else:
        return f"{total_bytes / 1024 ** 3:.2f} GB"

# === Query Executor ===
def run_query(sql):
    query_job = client.query(sql)
    return query_job.result().to_dataframe()



# === Assistant Mode Routing ===
if selected_mode == "GA4 Data Assistant":
    user_prompt = st.chat_input("Ask a question about your ecommerce data...")
    if user_prompt:
        st.session_state.has_started_chat = True
        st.chat_message("user").markdown(user_prompt)
        st.session_state.messages.append({"role": "user", "content": user_prompt})

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    raw_sql = generate_sql_prompt(
                        st.session_state.messages, user_prompt, selected_dataset, schema_type
                    )
                    estimated_size = estimate_query_size(raw_sql)

                    st.session_state.generated_sql = raw_sql
                    st.session_state.latest_user_prompt = user_prompt
                    st.session_state.estimated_size = estimated_size
                    st.session_state.awaiting_confirmation = True

                except Exception as e:
                    st.error(f"❌ Error preparing query: {e}")
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": f"❌ Error preparing query: {e}"
                    })

elif selected_mode == "Gemini Analytics Assistant":
    st.markdown("### 🤖 Gemini Analytics Q&A Agent")
    gemini_q = st.text_input("Ask a strategic or conceptual question (e.g., GA4 KPIs, funnel metrics):")
    if gemini_q:
        with st.spinner("Thinking with Gemini..."):
            try:
                answer = ask_gemini(gemini_q)
                st.info(answer)
            except Exception as e:
                st.error(f"Gemini error: {e}")


# === Query Confirmation Flow ===
if st.session_state.get("awaiting_confirmation", False):
    with st.chat_message("assistant"):
        st.code(st.session_state.generated_sql, language="sql")
        st.warning(f"⚠️ Estimated query cost: {st.session_state.estimated_size} of data will be scanned.")

        if st.button("Run Query (Accept Cost)"):
            with st.spinner("Running query..."):
                try:
                    df = run_query(st.session_state.generated_sql)
                    st.success("✅ Query ran successfully!")
                    st.dataframe(df)

                    with st.spinner("🧠 Generating insights..."):
                        summary = summarize_dataframe(df, st.session_state.latest_user_prompt)
                        st.markdown("### 📊 Insight Summary")
                        st.info(summary)

                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": f"Here is the result of your query:\n sql\n{st.session_state.generated_sql}\n\n{summary}"
                    })

                    # === CHART RECOMMENDATION AGENT ===
                    with st.spinner("🔍 Recommending chart type..."):
                        chart_agent = create_chart_agent()
                        df_sample = df.head(10).to_markdown(index=False)

                        try:
                            result = chart_agent.invoke({"input": df_sample})

                            # ✅ Safely extract chart recommendation
                            chart_text = getattr(result, "content", None)
                            if chart_text is None and isinstance(result, dict):
                                chart_text = result.get("output", "")

                            if chart_text:
                                chart_type = chart_text.strip().lower()
                                st.markdown(f"### 🧭 Recommended Chart Type: **{chart_type}**")

                                # === Auto-Render Chart ===
                                if chart_type == "line" or chart_type == "timeseries":
                                    st.line_chart(df.set_index(df.columns[0]))
                                elif chart_type == "bar":
                                    st.bar_chart(df.set_index(df.columns[0]))
                                else:
                                    st.info("ℹ️ Displaying table as no valid chart type was recognized.")
                                    st.dataframe(df)
                            else:
                                st.warning("🤖 Chart agent did not return a valid recommendation.")

                        except Exception as chart_err:
                            st.error(f"❌ Chart recommendation failed: {chart_err}")

                except Exception as e:
                    st.error(f"❌ Error executing query: {e}")
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": f"❌ Error executing query: {e}"
                    })

            st.session_state.awaiting_confirmation = False
