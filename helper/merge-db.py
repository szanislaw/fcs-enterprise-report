
import sqlite3
import shutil
import os

# Input DBs
input_dbs = {
    "cleaning": "cleaning.db",
    "location_status": "location_status.db",
    "job_order": "job_order.db",
    "staff": "staff.db"
}

# Output merged DB
output_db = "master-jo-co.db"

# Remove if it already exists
if os.path.exists(output_db):
    os.remove(output_db)

# Start with a copy of the first DB
shutil.copy(input_dbs["cleaning"], output_db)

# Connect to the merged DB
conn = sqlite3.connect(output_db)
cur = conn.cursor()

# Attach and copy tables from other DBs
for alias, path in input_dbs.items():
    if alias == "cleaning":
        continue  # already used as base
    cur.execute(f"ATTACH DATABASE '{path}' AS {alias};")
    tables = cur.execute(f"SELECT name FROM {alias}.sqlite_master WHERE type='table';").fetchall()
    for (table,) in tables:
        # Drop if exists in main to avoid conflicts
        cur.execute(f"DROP TABLE IF EXISTS main.{table};")
        cur.execute(f"CREATE TABLE main.{table} AS SELECT * FROM {alias}.{table};")
    cur.execute(f"DETACH DATABASE {alias};")

conn.commit()
conn.close()

print(f"✅ Merged database created: {output_db}")
