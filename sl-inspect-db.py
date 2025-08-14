import streamlit as st
import sqlite3
import pandas as pd

# Path to your SQLite database
DB_PATH = "db/master.db"

st.set_page_config(page_title="📊 DB Inspector", layout="wide")
st.title("📋 SQLite Database Inspector")

# Connect to the database
conn = sqlite3.connect(DB_PATH)

# Get all table names
tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)["name"].tolist()

if not tables:
    st.warning("No tables found in the database.")
    st.stop()

# Sidebar: select a table
table = st.sidebar.selectbox("📁 Select a table", tables)

# Display table schema
st.subheader(f"📘 Schema for `{table}`")
schema_df = pd.read_sql(f"PRAGMA table_info({table});", conn)
schema_df = schema_df.rename(columns={
    "cid": "Column ID", "name": "Column Name", "type": "Type", "notnull": "Not Null", "dflt_value": "Default", "pk": "Primary Key"
})
st.dataframe(schema_df, use_container_width=True)

# Show preview data
st.subheader(f"🔍 Preview data from `{table}`")
num_rows = st.slider("Number of rows to preview", 5, 100, 10)
df = pd.read_sql_query(f"SELECT * FROM {table} LIMIT {num_rows};", conn)
st.dataframe(df, use_container_width=True)

# Optional: search/filter
if not df.empty:
    st.markdown("### 🔎 Filter/Search")
    for col in df.columns:
        if df[col].dtype == "object":
            search_val = st.text_input(f"Search `{col}`", key=col)
            if search_val:
                df = df[df[col].astype(str).str.contains(search_val, case=False, na=False)]
                st.dataframe(df, use_container_width=True)

# Download button
st.download_button("📤 Download table as CSV", df.to_csv(index=False), file_name=f"{table}.csv")

conn.close()
