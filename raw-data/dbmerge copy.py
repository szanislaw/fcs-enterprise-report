import pandas as pd
import sqlite3
import os
import re

# ─────────────────────────────────────────────────────────────
# 0. Helper: Standardize Column Names
# ─────────────────────────────────────────────────────────────
def standardize_columns(df):
    rename_map = {
        "room": "room_number",
        "uuid ": "uuid",
        "staff id": "staff_id",
        "created by (user)": "created_by_user",
        "assigned to (user)": "assigned_to_user",
        "acknowledged by (user)": "acknowledged_by_user",
        "completed by (user)": "completed_by_user",
        "credit": "job_weight",  # 👈 Rename here
    }
    cleaned_cols = []
    for col in df.columns:
        col_clean = col.strip().lower()
        col_clean = re.sub(r"[^\w]+", "_", col_clean)
        col_clean = re.sub(r"_+", "_", col_clean)
        col_clean = col_clean.strip("_")
        col_clean = rename_map.get(col_clean, col_clean)
        cleaned_cols.append(col_clean)
    df.columns = cleaned_cols
    return df

# ─────────────────────────────────────────────────────────────
# 1. Load CSV files
# ─────────────────────────────────────────────────────────────
csv_files = {
    "co_cleaning_order": "co-cleaning-order.csv",
    "co_cleaning_order_detail": "co-cleaning-order-detail.csv",
    "co_cleaning_order_inspection": "co-cleaning-order-inspection.csv",
    "co_cleaning_order_map_additional_task": "co-cleaning-order-map-additional-task.csv",
    "co_cleaning_order_map_checklist": "co-cleaning-order-map-checklist.csv",
    "co_cleaning_order_checklist_detail": "co-cleaning_order_checklist_detail.csv",
    "co_location_category": "co-location_category.csv",
    "co_location_category_map_tag": "co-location-category-map-tag.csv",
    "co_location_indicator_audit_trail": "co-location-indicator-audit-trail.csv",
    "co_location_indicator_detail": "co-location-indicator-detail.csv",
    "co_matrix_detail": "co-matrix-detail.csv",
    "co_matrix_map_room_status": "co-matrix-map-room-status.csv",
    "co_matrix_map_user": "co-matrix-map-user.csv",
    "co_matrix_status": "co-matrix-status.csv",
    "co_service_type": "co-service-type.csv",
    "service_request": "service-request.csv",
    "payroll": "payroll.csv"
}

dfs = {}
for key, path in csv_files.items():
    if os.path.exists(path):
        df = pd.read_csv(path, low_memory=False)
        df = standardize_columns(df)
        dfs[key] = df
    else:
        print(f"⚠️ Missing file: {path}")

# ─────────────────────────────────────────────────────────────
# 2. Create `property` table
# ─────────────────────────────────────────────────────────────
property_map = {
    "2e76cf52-1334-4f22-9653-60b003b227b2": "Property 1",
    "4498c15d-50c5-4cf5-879a-dd5d674e7228": "Property 2"
}
dfs["property"] = pd.DataFrame([
    {"property_uuid": k, "property_name": v} for k, v in property_map.items()
])

# ─────────────────────────────────────────────────────────────
# 3. Add property_name to cleaning_order
# ─────────────────────────────────────────────────────────────
co_df = dfs["co_cleaning_order"]
co_df["property_name"] = co_df["property_uuid"].map(property_map)
dfs["co_cleaning_order"] = co_df

# ─────────────────────────────────────────────────────────────
# 4. Build `location` table
# ─────────────────────────────────────────────────────────────
location_df = co_df[["location_uuid"]].drop_duplicates()
location_df["room_number"] = None
location_map = co_df[["location_uuid", "property_uuid"]].drop_duplicates()
location_df = location_df.merge(location_map, on="location_uuid", how="left")
dfs["location"] = location_df

# ─────────────────────────────────────────────────────────────
# 5. Enrich service_request with property_name
# ─────────────────────────────────────────────────────────────
property_1_locations = {
    '2001', '2002', '2105', '2102', '2108', '2207', '2210', '6811',
    '2211', '2213', '2218', '2502', '2503', '6847', '6863', '6895'
}
property_2_locations = {
    '2207', '2301', '2302', '2303', '2305', '2306', '2307', '2308',
    '2310', '2311', '2313', '2315', '2316', '2318', '2319', '2320',
    '2321', '2322', '2323', '2324'
}
property_1_staff = {'HN RS3', 'HN RS2', 'HN RS1'}
property_2_staff = {'CN RS3', 'CN RS2', 'CN RS1'}

sr_df = dfs["service_request"]

def resolve_property(row):
    loc = str(row.get("room_number", "")).strip()
    staff_fields = [
        row.get("created_by_user", ""), row.get("assigned_to_user", ""),
        row.get("acknowledged_by_user", ""), row.get("completed_by_user", "")
    ]
    if loc in property_1_locations or any(s in property_1_staff for s in staff_fields):
        return "Property 1"
    elif loc in property_2_locations or any(s in property_2_staff for s in staff_fields):
        return "Property 2"
    return None

sr_df["property_name"] = sr_df.apply(resolve_property, axis=1)
dfs["service_request"] = sr_df

# ─────────────────────────────────────────────────────────────
# 6. Normalize staff and payroll (with names)
# ─────────────────────────────────────────────────────────────
payroll_df = dfs["payroll"]
if "uuid" not in payroll_df.columns:
    raise ValueError("Missing 'uuid' column in payroll.csv")

# From cleaning_order
co_staff = co_df[["assigned_uuid", "assigned_name", "acknowledged_uuid", "acknowledged_name", "completed_uuid", "completed_name"]].copy()
co_staff_long = pd.concat([
    co_staff[["assigned_uuid", "assigned_name"]].rename(columns={"assigned_uuid": "uuid", "assigned_name": "name"}),
    co_staff[["acknowledged_uuid", "acknowledged_name"]].rename(columns={"acknowledged_uuid": "uuid", "acknowledged_name": "name"}),
    co_staff[["completed_uuid", "completed_name"]].rename(columns={"completed_uuid": "uuid", "completed_name": "name"})
])

# From service_request (names only)
sr_staff_cols = ["created_by_user", "assigned_to_user", "acknowledged_by_user", "completed_by_user"]
existing_cols = [col for col in sr_staff_cols if col in sr_df.columns]
sr_names = sr_df[existing_cols].melt()["value"].dropna().unique()
sr_staff_long = pd.DataFrame({"uuid": [None]*len(sr_names), "name": sr_names})

# Combine all staff
staff_df = pd.concat([
    payroll_df[["uuid"]].assign(name=None),
    co_staff_long,
    sr_staff_long
], ignore_index=True)

# Deduplicate by name and UUID
staff_df = staff_df.dropna(subset=["name"]).drop_duplicates(subset=["uuid", "name"])
staff_df["property_name"] = staff_df["name"].apply(
    lambda x: "Property 1" if isinstance(x, str) and x.startswith("HN") else (
        "Property 2" if isinstance(x, str) and x.startswith("CN") else None
    )
)
staff_df = staff_df.drop_duplicates(subset=["name"])
dfs["staff"] = staff_df
dfs["payroll"] = payroll_df

# ─────────────────────────────────────────────────────────────
# 7. Normalize checklist-related tables
# ─────────────────────────────────────────────────────────────
if "co_cleaning_order_checklist_detail" in dfs:
    dfs["checklist_item"] = dfs["co_cleaning_order_checklist_detail"].rename(
        columns={"uuid": "checklist_id"}
    )

if "co_cleaning_order_map_checklist" in dfs:
    dfs["cleaning_order_checklist_map"] = dfs["co_cleaning_order_map_checklist"][[
        "cleaning_uuid", "checklist_uuid"
    ]].rename(columns={"checklist_uuid": "checklist_id"})

# ─────────────────────────────────────────────────────────────
# 8. Normalize location_category_tag_map
# ─────────────────────────────────────────────────────────────
if "co_location_category_map_tag" in dfs:
    tag_df = dfs["co_location_category_map_tag"]
    if "location_uuid" in tag_df.columns:
        tag_df = tag_df[tag_df["location_uuid"].notna()]
    dfs["location_category_tag_map"] = tag_df

# ─────────────────────────────────────────────────────────────
# 9. Write to SQLite
# ─────────────────────────────────────────────────────────────
conn = sqlite3.connect("master.db")
for name, df in dfs.items():
    table_name = name.replace("co_", "").replace("-", "_")
    df.to_sql(table_name, conn, if_exists="replace", index=False)
conn.commit()
conn.close()

print("✅ Normalized SQLite database created: master.db")
