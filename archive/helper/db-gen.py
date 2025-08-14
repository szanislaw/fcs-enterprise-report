
import os
import sqlite3
import pandas as pd

# Define the folder where your CSVs are stored
CSV_FOLDER = "raw-data"

# Utility to load CSV and create table
def load_csv_to_db(csv_name, db_path, table_name):
    df = pd.read_csv(os.path.join(CSV_FOLDER, csv_name))
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()

# Create cleaning.db
load_csv_to_db("co-cleaning-order.csv", "cleaning.db", "cleaning_order")
load_csv_to_db("co-cleaning-order-detail.csv", "cleaning.db", "cleaning_order_detail")
load_csv_to_db("co-cleaning-order-inspection.csv", "cleaning.db", "cleaning_order_inspection")
load_csv_to_db("co-cleaning-order-map-additional-task.csv", "cleaning.db", "cleaning_order_map_additional_task")
load_csv_to_db("co-cleaning-order-map-checklist.csv", "cleaning.db", "cleaning_order_map_checklist")
load_csv_to_db("co-cleaning_order_checklist_detail.csv", "cleaning.db", "checklist_detail")

# Create location_status.db
load_csv_to_db("co-location-indicator-detail.csv", "location_status.db", "location_indicator_detail")
load_csv_to_db("co-location-indicator-audit-trail.csv", "location_status.db", "location_indicator_audit_trail")
load_csv_to_db("co-location-category-map-tag.csv", "location_status.db", "location_category_map_tag")
load_csv_to_db("co-location_category.csv", "location_status.db", "location_category")
load_csv_to_db("co-matrix-detail.csv", "location_status.db", "matrix_detail")
load_csv_to_db("co-matrix-status.csv", "location_status.db", "matrix_status")
load_csv_to_db("co-matrix-map-room-status.csv", "location_status.db", "matrix_map_room_status")

# Create job_order.db
load_csv_to_db("jo-job-listing-july.csv", "job_order.db", "job_order")
load_csv_to_db("co-service-type.csv", "job_order.db", "service_type")

# Create staff.db
load_csv_to_db("payroll.csv", "staff.db", "staff")
load_csv_to_db("co-matrix-map-user.csv", "staff.db", "matrix_map_user")

print("✅ All 4 databases created: cleaning.db, location_status.db, job_order.db, staff.db")
