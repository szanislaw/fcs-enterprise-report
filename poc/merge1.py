import sqlite3
import pandas as pd
import os
import re

# Folder where your CSV files are stored
csv_folder = 'raw-data'
output_db = 'master.db'

def get_table_name(filename):
    base = os.path.splitext(filename)[0]
    if base.startswith('co-'):
        return base.replace('co-', 'cleaning_').replace('-', '_')
    elif base == 'service-requests':
        return 'service_request'
    elif base == 'payroll':
        return 'payroll'
    else:
        return base.replace('-', '_')

# Reset database
if os.path.exists(output_db):
    os.remove(output_db)

conn = sqlite3.connect(output_db)

for file in os.listdir(csv_folder):
    if file.endswith('.csv'):
        file_path = os.path.join(csv_folder, file)
        table_name = get_table_name(file)

        print(f"📥 Loading '{file}' → table `{table_name}`")

        try:
            df = pd.read_csv(file_path)

            # Clean columns
            cleaned_columns = [
                re.sub(r'\W|^(?=\d)', '_', col.strip().lower())
                for col in df.columns
            ]
            df.columns = cleaned_columns

            # 🛠 Rename UUID → user_uuid for payroll
            if table_name == 'payroll':
                print("📛 Payroll columns before rename:", df.columns.tolist())
                for col in df.columns:
                    if col.strip().lower() == 'uuid':
                        df.rename(columns={col: 'user_uuid'}, inplace=True)
                        print("✅ Renamed column", col, "→ user_uuid")
                print("📛 Payroll columns after rename:", df.columns.tolist())

            df.to_sql(table_name, conn, index=False, if_exists='replace')

        except Exception as e:
            print(f"❌ Failed to import '{file}': {e}")

conn.close()
print(f"✅ All CSVs successfully imported into '{output_db}'")
