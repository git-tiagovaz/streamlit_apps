# pages/02_Gemini_Analytics_Agent.py

import streamlit as st
from vertexai.language_models import ChatModel

st.set_page_config(page_title="Gemini Analytics Assistant", layout="wide")
st.title("🧠 Gemini Analytics Assistant")
st.markdown("Use this agent to ask general analytics questions — e.g. key GA4 metrics, funnel strategies, tracking setups.")

# === Input ===
user_question = st.text_input("What would you like to know?")

# === Gemini Interaction ===
def ask_gemini(question):
    model = ChatModel.from_pretrained("gemini-1.5-pro")  # Replace with "gemini-2.5" when available
    chat = model.start_chat()

    full_prompt = f"""
You are a senior digital analytics expert specializing in ecommerce and GA4 data.
Answer the following question in a helpful, strategic, and plain-English manner:

"{question}"

If the question is vague, suggest how the user could refine it. Provide examples and insights drawn from GA4 best practices.
"""

    response = chat.send_message(full_prompt)
    return response.text

# === Run ===
if user_question:
    with st.spinner("Thinking with Gemini..."):
        try:
            gemini_answer = ask_gemini(user_question)
            st.markdown("---")
            st.markdown("### 🤖 Gemini’s Answer")
            st.info(gemini_answer)
        except Exception as e:
            st.error(f"Gemini Agent Error: {e}")
