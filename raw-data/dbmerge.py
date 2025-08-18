import pandas as pd
import sqlite3
import re

# ─────────────────────────────────────────────────────────────
# Helper: Standardize column names
# ─────────────────────────────────────────────────────────────
def clean_column_name(name):
    name = name.strip().lower()
    name = re.sub(r'[^0-9a-zA-Z]+', '_', name)   # replace spaces/symbols with _
    name = re.sub(r'_+', '_', name)              # collapse multiple underscores
    return name.strip('_')

# ─────────────────────────────────────────────────────────────
# Load CSVs
# ─────────────────────────────────────────────────────────────
cleaning_orders = pd.read_csv("cleaning-orders.csv")
payroll = pd.read_csv("payroll.csv")
service_requests = pd.read_csv("service-requests.csv")

# Clean column names
cleaning_orders.columns = [clean_column_name(c) for c in cleaning_orders.columns]
payroll.columns = [clean_column_name(c) for c in payroll.columns]
service_requests.columns = [clean_column_name(c) for c in service_requests.columns]

# ─────────────────────────────────────────────────────────────
# Build SQLite database
# ─────────────────────────────────────────────────────────────
conn = sqlite3.connect("hotel_operations.db")
cur = conn.cursor()

# Drop old tables if exist
tables = ["staff", "properties", "payroll", "cleaning_orders", "service_requests"]
for t in tables:
    cur.execute(f"DROP TABLE IF EXISTS {t};")

# ─────────────────────────────────────────────────────────────
# Create schema
# ─────────────────────────────────────────────────────────────

cur.execute("""
CREATE TABLE properties (
    property_id TEXT PRIMARY KEY,
    property_name TEXT
);
""")

cur.execute("""
CREATE TABLE staff (
    staff_id TEXT PRIMARY KEY,
    staff_name TEXT,
    nationality TEXT,
    job_title TEXT,
    employment_type TEXT,
    property_id TEXT,
    FOREIGN KEY(property_id) REFERENCES properties(property_id)
);
""")

cur.execute("""
CREATE TABLE payroll (
    payroll_id INTEGER PRIMARY KEY AUTOINCREMENT,
    staff_id TEXT,
    pay_period_start TEXT,
    pay_period_end TEXT,
    pay_frequency TEXT,
    gross_pay REAL,
    net_pay REAL,
    cpf_contribution REAL,
    bonuses REAL,
    FOREIGN KEY(staff_id) REFERENCES staff(staff_id)
);
""")

cur.execute("""
CREATE TABLE cleaning_orders (
    order_id INTEGER PRIMARY KEY AUTOINCREMENT,
    staff_id TEXT,
    cleaning_service_type TEXT,
    property_id TEXT,
    location_uuid TEXT,
    location_name TEXT,
    start_time TEXT,
    complete_time TEXT,
    duration TEXT,
    inspector_name TEXT,
    inspection_result TEXT,
    FOREIGN KEY(staff_id) REFERENCES staff(staff_id),
    FOREIGN KEY(property_id) REFERENCES properties(property_id)
);
""")

cur.execute("""
CREATE TABLE service_requests (
    request_id TEXT PRIMARY KEY,
    guest_name TEXT,
    location TEXT,
    service_category TEXT,
    service_item TEXT,
    quantity INTEGER,
    remarks TEXT,
    status TEXT,
    created_time TEXT,
    deadline_time TEXT,
    completed_time TEXT,
    assigned_staff_id TEXT,
    FOREIGN KEY(assigned_staff_id) REFERENCES staff(staff_id)
);
""")

# ─────────────────────────────────────────────────────────────
# Insert data into properties & staff
# ─────────────────────────────────────────────────────────────
properties_df = payroll[["property_uuid", "property_name"]].drop_duplicates()
properties_df.rename(columns={"property_uuid": "property_id"}, inplace=True)
properties_df.to_sql("properties", conn, if_exists="append", index=False)

staff_df = payroll.rename(columns={
    "uuid": "staff_id",
    "employee_name": "staff_name"
})[["staff_id", "staff_name", "nationality", "job_title", "employment_type", "property_uuid"]]
staff_df.rename(columns={"property_uuid": "property_id"}, inplace=True)
staff_df.to_sql("staff", conn, if_exists="append", index=False)

# ─────────────────────────────────────────────────────────────
# Insert payroll data
# ─────────────────────────────────────────────────────────────
payroll_df = payroll.rename(columns={
    "uuid": "staff_id",
    "payroll_period_start": "pay_period_start",
    "payroll_period_end": "pay_period_end",
    "pay_frequency": "pay_frequency",
    "gross_pay_sgd": "gross_pay",
    "net_pay_sgd": "net_pay",
    "cpf_contribution_sgd": "cpf_contribution",
    "performance_bonus_sgd": "bonuses"
})[["staff_id", "pay_period_start", "pay_period_end", "pay_frequency", "gross_pay", "net_pay", "cpf_contribution", "bonuses"]]

payroll_df.to_sql("payroll", conn, if_exists="append", index=False)

# ─────────────────────────────────────────────────────────────
# Insert cleaning orders
# ─────────────────────────────────────────────────────────────
cleaning_df = cleaning_orders.rename(columns={
    "staff_uuid": "staff_id",
    "property": "property_id",
    "cleaning_service_type": "cleaning_service_type",
    "start_time": "start_time",
    "complete_time": "complete_time",
    "cleaning_duration": "duration",
    "inspector": "inspector_name",
    "pass_fail": "inspection_result"
})[["staff_id", "cleaning_service_type", "property_id", "location_uuid",
    "location_name", "start_time", "complete_time", "duration", "inspector_name", "inspection_result"]]

cleaning_df.to_sql("cleaning_orders", conn, if_exists="append", index=False)

# ─────────────────────────────────────────────────────────────
# Insert service requests
# ─────────────────────────────────────────────────────────────
service_df = service_requests.rename(columns={
    "job_order": "request_id",
    "job_status": "status",
    "date_time_created": "created_time",
    "date_time_deadline": "deadline_time",
    "date_time_completed": "completed_time",
    "assigned_to_user": "assigned_staff_id",
    "service_item_category": "service_category"
})[["request_id", "guest_name", "location", "service_category", "service_item", "quantity",
    "remarks", "status", "created_time", "deadline_time", "completed_time", "assigned_staff_id"]]

service_df.to_sql("service_requests", conn, if_exists="append", index=False)

# Commit & close
conn.commit()
conn.close()

print("✅ SQLite database 'hotel_operations.db' created successfully!")
