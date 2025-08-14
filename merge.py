import sqlite3
import pandas as pd
import os
import re

# Folder where your CSV files are stored
csv_folder = 'raw-data'  # Change this path if your CSVs are in a different directory
output_db = 'master.db'

# Table name mapping logic
def get_table_name(filename):
    base = os.path.splitext(filename)[0]
    if base.startswith('co-'):
        return base.replace('co-', 'cleaning_').replace('-', '_')
    elif base == 'service-requests':
        return 'service_requests'
    elif base == 'payroll':
        return 'payroll'
    else:
        return base.replace('-', '_')

# Reset database if it already exists
if os.path.exists(output_db):
    os.remove(output_db)

conn = sqlite3.connect(output_db)

# Loop through all CSV files in folder
for file in os.listdir(csv_folder):
    if file.endswith('.csv'):
        file_path = os.path.join(csv_folder, file)
        table_name = get_table_name(file)

        print(f"📥 Loading '{file}' → table {table_name}")

        try:
            df = pd.read_csv(file_path)
            df.columns = [
                re.sub(r'\W|^(?=\d)', '_', col.strip().lower())
                for col in df.columns
            ]
            df.to_sql(table_name, conn, index=False, if_exists='replace')
        except Exception as e:
            print(f"❌ Failed to import '{file}': {e}")

conn.close()
print(f"✅ All CSVs successfully imported into '{output_db}'")