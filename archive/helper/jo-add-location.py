import sqlite3

# Property-to-location mapping
property_map = {
    "Property 1": [
        "2001", "2002", "2105", "2102", "2108", "2207", "2210", "6811", "2211", "2213",
        "2218", "2502", "2503", "6811", "6847", "6863", "6895"
    ],
    "Property 2": [
        "2207", "2301", "2302", "2303", "2305", "2306", "2307", "2308", "2310", "2311",
        "2313", "2315", "2316", "2318", "2319", "2320", "2321", "2322", "2323", "2324"
    ]
}

# Property-to-staff mapping
property_staff = {
    "Property 1": ["HN RS3", "HN RS2", "HN RS1"],
    "Property 2": ["CN RS3", "CN RS2", "CN RS1"]
}

# Connect to the database
conn = sqlite3.connect("db/master-jo-co.db")
cur = conn.cursor()

# Create property table (property → location)
cur.execute("DROP TABLE IF EXISTS property;")
cur.execute("""
    CREATE TABLE property (
        property_id TEXT,
        location_id TEXT
    );
""")

# Create property_staff table (property → staff)
cur.execute("DROP TABLE IF EXISTS property_staff;")
cur.execute("""
    CREATE TABLE property_staff (
        property_id TEXT,
        staff_id TEXT
    );
""")

# Populate property-location mappings
for prop, locations in property_map.items():
    for loc in locations:
        cur.execute("INSERT INTO property (property_id, location_id) VALUES (?, ?);", (prop, loc))

# Populate property-staff mappings
for prop, staff_list in property_staff.items():
    for staff in staff_list:
        cur.execute("INSERT INTO property_staff (property_id, staff_id) VALUES (?, ?);", (prop, staff))

conn.commit()
conn.close()

print("✅ Tables `property` and `property_staff` created and populated.")
