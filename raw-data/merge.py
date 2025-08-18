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
# Property mapping by location
# ─────────────────────────────────────────────────────────────
property1_locations = {"2001","2002","2105","2102","2108","2207","2210","2211","2213",
                       "2218","2502","2503","6811","6847","6863","6895"}
property2_locations = {"2207","2301","2302","2303","2305","2306","2307","2308","2310",
                       "2311","2313","2315","2316","2318","2319","2320","2321","2322",
                       "2323","2324","88888888"}

def map_property(loc):
    if pd.isna(loc):
        return "Property 1"  # default
    loc = str(loc).strip()
    if loc in property2_locations:
        return "Property 2"
    return "Property 1"

service_requests["prop_name"] = service_requests["location"].apply(map_property)
service_requests["prop_id"] = service_requests["prop_name"].apply(lambda x: "P1" if x == "Property 1" else "P2")

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
    prop_id TEXT PRIMARY KEY,
    prop_name TEXT
);
""")

cur.execute("""
CREATE TABLE staff (
    stf_id TEXT PRIMARY KEY,
    stf_name TEXT,
    nationality TEXT,
    job_title TEXT,
    employment_type TEXT,
    prop_id TEXT,
    FOREIGN KEY(prop_id) REFERENCES properties(prop_id)
);
""")

cur.execute("""
CREATE TABLE payroll (
    pay_id INTEGER PRIMARY KEY AUTOINCREMENT,
    stf_id TEXT,
    pay_period_start TEXT,
    pay_period_end TEXT,
    pay_frequency TEXT,
    gross_pay REAL,
    net_pay REAL,
    cpf_contribution REAL,
    bonuses REAL,
    FOREIGN KEY(stf_id) REFERENCES staff(stf_id)
);
""")

cur.execute("""
CREATE TABLE cleaning_orders (
    co_id INTEGER PRIMARY KEY AUTOINCREMENT,
    stf_id TEXT,
    cleaning_service_type TEXT,
    prop_id TEXT,
    location_uuid TEXT,
    location_name TEXT,
    start_time TEXT,
    complete_time TEXT,
    duration TEXT,
    inspector_name TEXT,
    inspection_result TEXT,
    FOREIGN KEY(stf_id) REFERENCES staff(stf_id),
    FOREIGN KEY(prop_id) REFERENCES properties(prop_id)
);
""")

cur.execute("""
CREATE TABLE service_requests (
    sr_id TEXT PRIMARY KEY,
    guest_name TEXT,
    location TEXT,
    prop_id TEXT,
    service_category TEXT,
    service_item TEXT,
    quantity INTEGER,
    remarks TEXT,
    status TEXT,
    created_time TEXT,
    deadline_time TEXT,
    completed_time TEXT,
    assigned_stf_id TEXT,
    FOREIGN KEY(assigned_stf_id) REFERENCES staff(stf_id),
    FOREIGN KEY(prop_id) REFERENCES properties(prop_id)
);
""")

# ─────────────────────────────────────────────────────────────
# Insert data into properties & staff
# ─────────────────────────────────────────────────────────────
properties_df = payroll[["property_uuid", "property_name"]].drop_duplicates()
properties_df.rename(columns={
    "property_uuid": "prop_id",
    "property_name": "prop_name"
}, inplace=True)
properties_df.to_sql("properties", conn, if_exists="append", index=False)

staff_df = payroll.rename(columns={
    "uuid": "stf_id",
    "employee_name": "stf_name",
    "property_uuid": "prop_id"
})[["stf_id", "stf_name", "nationality", "job_title", "employment_type", "prop_id"]]
staff_df.to_sql("staff", conn, if_exists="append", index=False)

# ─────────────────────────────────────────────────────────────
# Insert payroll data
# ─────────────────────────────────────────────────────────────
payroll_df = payroll.rename(columns={
    "uuid": "stf_id",
    "payroll_period_start": "pay_period_start",
    "payroll_period_end": "pay_period_end",
    "pay_frequency": "pay_frequency",
    "gross_pay_sgd": "gross_pay",
    "net_pay_sgd": "net_pay",
    "cpf_contribution_sgd": "cpf_contribution",
    "performance_bonus_sgd": "bonuses"
})[["stf_id", "pay_period_start", "pay_period_end", "pay_frequency", "gross_pay", "net_pay", "cpf_contribution", "bonuses"]]

payroll_df.to_sql("payroll", conn, if_exists="append", index=False)

# ─────────────────────────────────────────────────────────────
# Insert cleaning orders
# ─────────────────────────────────────────────────────────────
cleaning_df = cleaning_orders.rename(columns={
    "staff_uuid": "stf_id",
    "property": "prop_id",
    "cleaning_service_type": "cleaning_service_type",
    "start_time": "start_time",
    "complete_time": "complete_time",
    "cleaning_duration": "duration",
    "inspector": "inspector_name",
    "pass_fail": "inspection_result"
})[["stf_id", "cleaning_service_type", "prop_id", "location_uuid",
    "location_name", "start_time", "complete_time", "duration", "inspector_name", "inspection_result"]]

cleaning_df.to_sql("cleaning_orders", conn, if_exists="append", index=False)

# ─────────────────────────────────────────────────────────────
# Insert service requests (with prop_id)
# ─────────────────────────────────────────────────────────────
service_df = service_requests.rename(columns={
    "job_order": "sr_id",
    "job_status": "status",
    "date_time_created": "created_time",
    "date_time_deadline": "deadline_time",
    "date_time_completed": "completed_time",
    "assigned_to_user": "assigned_stf_id",
    "service_item_category": "service_category"
})[["sr_id", "guest_name", "location", "prop_id", "service_category", "service_item", "quantity",
    "remarks", "status", "created_time", "deadline_time", "completed_time", "assigned_stf_id"]]

service_df.to_sql("service_requests", conn, if_exists="append", index=False)

# Commit & close
conn.commit()
conn.close()

print("✅ SQLite database 'hotel_operations.db' created successfully with prop_id in service_requests!")
