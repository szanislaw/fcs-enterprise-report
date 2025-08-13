import streamlit as st
import sqlite3
import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM
import altair as alt

# Load SQLCoder model
@st.cache_resource
def load_model():
    tokenizer = AutoTokenizer.from_pretrained("defog/sqlcoder-7b-2")
    model = AutoModelForCausalLM.from_pretrained(
        "defog/sqlcoder-7b-2",
        torch_dtype=torch.float16,
        device_map="auto"
    )
    return tokenizer, model

tokenizer, model = load_model()

# Connect to SQLite DB
conn = sqlite3.connect("db/master-jo-co.db")

# Show schema to user
def get_schema():
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)["name"].tolist()
    schema = ""
    for table in tables:
        cols = pd.read_sql(f"PRAGMA table_info({table});", conn)
        col_list = ", ".join(cols['name'])
        schema += f"Table `{table}`: {col_list}\n"
    return schema

schema_text = get_schema()

# Translate NLQ to SQL
def nl_to_sql(nlq):
    prompt = f"""### Database Schema
{schema_text}

### Task
Generate a SQL query to answer the following question according to the schema above:
{nlq}
"""
    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
    outputs = model.generate(
        **inputs,
        max_new_tokens=256,
        temperature=0.7,
        top_p=0.9,
        pad_token_id=tokenizer.pad_token_id or tokenizer.eos_token_id,
    )
    raw = tokenizer.decode(outputs[0], skip_special_tokens=True)
    return raw.strip().split("\n")[-1]  # last line is often the SQL

# UI layout
st.set_page_config(layout="wide")
st.title("🧠 SQLCoder Query Assistant for Cleaning Facility DB")

with st.expander("📘 View Database Schema"):
    st.code(schema_text)

question = st.text_input("🔎 Ask a question about the database:")

if question:
    sql_query = nl_to_sql(question)
    st.code(sql_query, language="sql")

    try:
        df = pd.read_sql(sql_query, conn)
        st.success(f"✅ Query returned {len(df)} rows")
        st.dataframe(df, use_container_width=True)

        # Optional chart
        if not df.empty and df.select_dtypes(include=["number"]).shape[1] >= 1:
            st.subheader("📊 Quick Chart")
            numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
            other_cols = df.select_dtypes(exclude=["number"]).columns.tolist()

            if numeric_cols:
                x_axis = st.selectbox("X-axis", other_cols if other_cols else numeric_cols)
                y_axis = st.selectbox("Y-axis", numeric_cols)

                chart = alt.Chart(df).mark_bar().encode(
                    x=x_axis,
                    y=y_axis,
                    tooltip=list(df.columns)
                ).interactive()

                st.altair_chart(chart, use_container_width=True)

    except Exception as e:
        st.error(f"❌ Query failed: {e}")
