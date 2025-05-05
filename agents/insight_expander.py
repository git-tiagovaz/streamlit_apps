from dotenv import load_dotenv
import os
import openai

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY

def suggest_additional_insights(user_question: str, df_sample: str) -> str:
    prompt = (
        f"You are a Senior Google Analytics 4 data analyst. A user asked the following question:\n"
        f"{user_question}\n\n"
        f"Below is a sample of the GA4 BigQuery query result:\n"
        f"{df_sample}\n\n"
        "Your task is to:\n"
        "1. Suggest 2 to 3 deeper or related follow-up questions the user could ask next to further investigate the topic.\n"
        "2. These suggestions should help uncover patterns, causes, or optimization opportunities (e.g. segments, performance gaps).\n"
        "3. Keep it relevant to ecommerce and GA4 raw data.\n"
        "4. Output only the suggestions as a numbered list, no extra explanation."
    )

    response = openai.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7
    )

    return response.choices[0].message.content.strip()
