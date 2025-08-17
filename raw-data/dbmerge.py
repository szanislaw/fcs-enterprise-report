import pandas as pd
import sqlite3
import os
import uuid

# ──────────────────────────────────────────────
# Property mappings
# ──────────────────────────────────────────────
PROPERTY_LOC_MAP = {
    "Property 1": [
        "2001","2002","2105","2102","2108","2207","2210","6811",
        "2211","2213","2218","2502","2503","6847","6863","6895"
    ],
    "Property 2": [
        "2207","2301","2302","2303","2305","2306","2307","2308",
        "2310","2311","2313","2315","2316","2318","2319","2320",
        "2321","2322","2323","2324"
    ]
}

PROPERTY_STAFF_MAP = {
    "Property 1": ["HN RS1","HN RS2","HN RS3"],
    "Property 2": ["CN RS1","CN RS2","CN RS3"]
}

PROPERTY_UUID_MAP = {
    "2e76cf52-1334-4f22-9653-60b003b227b2": "Property 1",
    "4498c15d-50c5-4cf5-879a-dd5d674e7228": "Property 2"
}

# ──────────────────────────────────────────────
# Helper: Clean generic column names
# ──────────────────────────────────────────────
def clean_columns(df):
    df.columns = (
        df.columns.str.strip()
        .str.replace(" ", "_")
        .str.replace("&", "and")
        .str.replace("(", "")
        .str.replace(")", "")
        .str.replace("-", "_")
        .str.lower()
    )
    return df

# ──────────────────────────────────────────────
# Load CSVs
# ──────────────────────────────────────────────
files = {
    "payroll": "payroll.csv",
    "service_requests": "service-requests.csv",
    "co_service_type": "co-service-type.csv",
    "co_matrix_status": "co-matrix-status.csv",
    "co_matrix_map_user": "co-matrix-map-user.csv",
    "co_matrix_map_room_status": "co-matrix-map-room-status.csv",
    "co_matrix_detail": "co-matrix-detail.csv",
    "co_location_indicator_detail": "co-location-indicator-detail.csv",
    "co_location_indicator_audit_trail": "co-location-indicator-audit-trail.csv",
    "co_location_category_map_tag": "co-location-category-map-tag.csv",
    "co_location_category": "co-location_category.csv",
    "co_cleaning_order_map_checklist": "co-cleaning-order-map-checklist.csv",
    "co_cleaning_order_map_additional_task": "co-cleaning-order-map-additional-task.csv",
    "co_cleaning_order_inspection": "co-cleaning-order-inspection.csv",
    "co_cleaning_order_detail": "co-cleaning-order-detail.csv",
    "co_cleaning_order_checklist_detail": "co-cleaning_order_checklist_detail.csv",
    "co_cleaning_order": "co-cleaning-order.csv",
}

dfs = {}
for key, path in files.items():
    df = pd.read_csv(os.path.join("", path))
    dfs[key] = clean_columns(df)

# ──────────────────────────────────────────────
# Build Property Table
# ──────────────────────────────────────────────
property_table = pd.DataFrame([
    {"property_id": 1, "property_name": "Property 1", "property_uuid": "2e76cf52-1334-4f22-9653-60b003b227b2"},
    {"property_id": 2, "property_name": "Property 2", "property_uuid": "4498c15d-50c5-4cf5-879a-dd5d674e7228"},
])

# ──────────────────────────────────────────────
# Payroll → Staff table
# ──────────────────────────────────────────────
payroll = dfs["payroll"].rename(columns={
    "uuid": "staff_id",
    "employee_name": "staff_name",
    "gross_pay_sgd": "gross_pay",
    "net_pay_sgd": "net_pay"
})

payroll["property_id"] = payroll["property_uuid"].map(
    {v: k for k, v in enumerate(PROPERTY_UUID_MAP.values(), start=1)}
)

staff_table = payroll[["staff_id","staff_name","property_id"]].drop_duplicates()

# ──────────────────────────────────────────────
# Room Service Requests
# ──────────────────────────────────────────────
rsr = dfs["service_requests"].rename(columns={
    "dateand_time_created": "created_at",
    "dateand_time_completed": "completed_at",
    "service_item_category": "service_category",
    "service_item": "service_item",
    "job_status": "status",
    "job_order": "request_id",
    "assigned_to_user": "assigned_to"
})

if "request_id" not in rsr.columns:
    rsr.insert(0, "request_id", [str(uuid.uuid4()) for _ in range(len(rsr))])

def assign_property_rsr(row):
    loc = str(row.get("location",""))
    staff = str(row.get("assigned_to",""))
    for prop, locs in PROPERTY_LOC_MAP.items():
        if loc in locs:
            return 1 if prop == "Property 1" else 2
    for prop, staff_list in PROPERTY_STAFF_MAP.items():
        if staff in staff_list:
            return 1 if prop == "Property 1" else 2
    return None

rsr["property_id"] = rsr.apply(assign_property_rsr, axis=1)

# ──────────────────────────────────────────────
# Cleaning Orders
# ──────────────────────────────────────────────
co = dfs["co_cleaning_order"].rename(columns={
    "cleaning_uuid": "order_id",
    "created_date": "created_at",
    "completed_date": "completed_at",
    "assigned_name": "assigned_to",
    "completed_name": "completed_by"
})

def assign_property_co(row):
    puid = str(row.get("property_uuid",""))
    pname = PROPERTY_UUID_MAP.get(puid, None)
    if pname == "Property 1":
        return 1
    elif pname == "Property 2":
        return 2
    return None

co["property_id"] = co.apply(assign_property_co, axis=1)

# ──────────────────────────────────────────────
# Create SQLite DB
# ──────────────────────────────────────────────
db_path = "normalized_sqlcoder.db"
if os.path.exists(db_path):
    os.remove(db_path)

conn = sqlite3.connect(db_path)

# Core tables
property_table.to_sql("property", conn, if_exists="replace", index=False)
staff_table.to_sql("staff", conn, if_exists="replace", index=False)
payroll.to_sql("payroll", conn, if_exists="replace", index=False)
rsr.to_sql("room_service_requests", conn, if_exists="replace", index=False)
co.to_sql("cleaning_orders", conn, if_exists="replace", index=False)

# Supporting co_* tables (with cleaned column names)
for key, df in dfs.items():
    if key not in ["payroll","service_requests","co_cleaning_order"]:
        df.to_sql(key, conn, if_exists="replace", index=False)

conn.commit()
conn.close()

print("✅ SQLCoder-optimized database created as", db_path)
