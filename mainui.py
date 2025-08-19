import streamlit as st
import sqlite3
import pandas as pd
import torch
import time
import altair as alt
import re
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
import os

# # ─────────────────────────────────────────────────────────────────────────────
# # Model Loading with Timing
# # ─────────────────────────────────────────────────────────────────────────────
model_load_start = time.time()

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
model_load_time = time.time() - model_load_start

# model_load_start = time.time()

# @st.cache_resource
# def load_model():
#     tokenizer = AutoTokenizer.from_pretrained("defog/sqlcoder-7b-2")

#     # Quantisation config (8-bit for balance, switch to 4-bit if VRAM is tighter)
#     quant_config = BitsAndBytesConfig(
#         load_in_8bit=False,      # set to False and use load_in_4bit=True for max VRAM savings
#         load_in_4bit=True,
#         llm_int8_threshold=6.0, # keeps accuracy better by not quantising outliers
#         llm_int8_has_fp16_weight=True
#     )

#     model = AutoModelForCausalLM.from_pretrained(
#         "defog/sqlcoder-7b-2",
#         quantization_config=quant_config,
#         device_map="auto"
#     )
#     return tokenizer, model

# tokenizer, model = load_model()
# model_load_time = time.time() - model_load_start

# ─────────────────────────────────────────────────────────────────────────────
# Load Prompt Template
# ─────────────────────────────────────────────────────────────────────────────
with open("prompt/prompt.txt", "r", encoding="utf-8") as f:
    prompt_template = f.read()

# ─────────────────────────────────────────────────────────────────────────────
# Connect to SQLite DB and Schema Fetching
# ─────────────────────────────────────────────────────────────────────────────
conn = sqlite3.connect("db/master.db")

@st.cache_data
def get_schema():
    tables = pd.read_sql(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';",
        conn)["name"].tolist()
    
    schema_md = ""
    for table in tables:
        cols = pd.read_sql(f"PRAGMA table_info({table});", conn)
        schema_md += f"### Table `{table}`\n"
        schema_md += "| Column | Type |\n|--------|------|\n"
        for _, col in cols.iterrows():
            schema_md += f"| `{col['name']}` | `{col['type']}` |\n"
        schema_md += "\n"
    return schema_md

schema_text = get_schema()

# ─────────────────────────────────────────────────────────────────────────────
# SQL Extraction Helper
# ─────────────────────────────────────────────────────────────────────────────
def extract_sql_from_output(output):
    match = re.search(r"```sql\n(.*?)```", output, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    match = re.search(r"(SELECT .*?;)", output, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return output.strip().split("\n")[-1].strip()

# ─────────────────────────────────────────────────────────────────────────────
# SQL Fix Layer for SQLite Compatibility
# ─────────────────────────────────────────────────────────────────────────────
def apply_sql_fixes(sql):
    # Fix ILIKE and LIKE
    sql = re.sub(r"(\b\w+\.\w+)\s+ILIKE\s+'([^']*)'", lambda m: f"LOWER({m.group(1)}) LIKE LOWER('%{m.group(2)}%')", sql, flags=re.IGNORECASE)
    sql = re.sub(r"(\b\w+\.\w+)\s+LIKE\s+'([^']*)'", lambda m: f"LOWER({m.group(1)}) LIKE LOWER('%{m.group(2)}%')", sql, flags=re.IGNORECASE)

    # Replace EXTRACT with strftime (Month)
    sql = re.sub(r"EXTRACT\s*\(\s*MONTH\s+FROM\s+([^)]+?)\s*\)", r"CAST(strftime('%m', \1) AS INTEGER)", sql)
    sql = re.sub(r"EXTRACT\s*\(\s*YEAR\s+FROM\s+([^)]+?)\s*\)", r"CAST(strftime('%Y', \1) AS INTEGER)", sql)

    # Remove PostgreSQL-style casts ::DATE
    sql = re.sub(r"::\s*DATE", "", sql)

    # Optional: Remove double-quoted column names if any
    sql = sql.replace('"', '')

    return sql

# ─────────────────────────────────────────────────────────────────────────────
# NLQ to SQL Translation
# ─────────────────────────────────────────────────────────────────────────────
def nl_to_sql(nlq):
    prompt = prompt_template.format(question=nlq, schema=schema_text)
    start = time.time()
    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
    outputs = model.generate(
        **inputs,
        max_new_tokens=256,
        temperature=0.7,
        top_p=0.9,
        pad_token_id=tokenizer.pad_token_id or tokenizer.eos_token_id,
    )
    raw = tokenizer.decode(outputs[0], skip_special_tokens=True)
    sql = extract_sql_from_output(raw)
    sql = apply_sql_fixes(sql)
    st.session_state["sqlgen_time"] = time.time() - start

    print("💬 Prompt:\n", prompt)
    print("🧠 Raw Output:\n", raw)
    print("🛠️ Fixed SQL:\n", sql)

    return sql

# ─────────────────────────────────────────────────────────────────────────────
# Streamlit UI
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(layout="wide")
st.title("🧐 SQLCoder Query Assistant for Hotel Operations")
st.markdown(f"<p style='font-size: 0.8rem; color: gray;'>Model load time: {model_load_time:.4f} seconds</p>", unsafe_allow_html=True)

with st.expander("📘 View Database Schema"):
    st.code(schema_text)

question = st.text_input("🔎 Ask a question about the database:")

if question:
    total_start = time.time()
    sql_query = nl_to_sql(question)
    st.code(sql_query, language="sql")

    try:
        query_start = time.time()
        df = pd.read_sql(sql_query, conn)
        st.session_state["query_time"] = time.time() - query_start

        # Charting
        chart_start = time.time()
        chart_rendered = False
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
                chart_rendered = True

        st.session_state["chart_time"] = time.time() - chart_start if chart_rendered else 0.0
        st.success(f"✅ Query returned {len(df)} rows")
        st.dataframe(df, use_container_width=True)

    except Exception as e:
        st.error(f"❌ Query failed: {e}")

    total_time = time.time() - total_start
    st.markdown("""
    <div style='font-size: 0.8rem; color: gray;'>
        📝 SQL Generation Time: {:.4f} s &nbsp; | &nbsp;
        🧰 Query Execution Time: {:.4f} s &nbsp; | &nbsp;
        📊 Chart Render Time: {:.4f} s &nbsp; | &nbsp;
        ⏱️ Total Time: {:.4f} s
    </div>
    """.format(
        st.session_state.get("sqlgen_time", 0),
        st.session_state.get("query_time", 0),
        st.session_state.get("chart_time", 0),
        total_time
    ), unsafe_allow_html=True)
