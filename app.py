import streamlit as st
from crew_config import run_crew

# Required session state or constants
BQ_TABLE = "events_*"
BQ_PROJECT_ID = st.secrets["gcp_service_account"]["project_id"]

# User input
user_prompt = st.chat_input("Ask a question...")
selected_dataset = "analytics_123456"  # Replace this or fetch dynamically

if user_prompt:
    st.chat_message("user").markdown(user_prompt)
    st.session_state.messages.append({"role": "user", "content": user_prompt})

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                result = run_crew(
                    user_prompt,
                    selected_dataset=selected_dataset,
                    bq_table=BQ_TABLE,
                    project_id=BQ_PROJECT_ID
                )
                st.success("✅ Task complete!")
                st.markdown(result)

                st.session_state.messages.append({
                    "role": "assistant",
                    "content": result
                })
            except Exception as e:
                st.error(f"❌ Error: {e}")
