import sqlite3

conn = sqlite3.connect("db/master.db")
cur = conn.cursor()

# Step 1: Create Property table
cur.execute("""
CREATE TABLE IF NOT EXISTS property (
    property_uuid TEXT PRIMARY KEY
);
""")

# Step 2: Create Room table
cur.execute("""
CREATE TABLE IF NOT EXISTS room (
    room_id TEXT PRIMARY KEY
);
""")

# Step 3: Create Room Assignment table
cur.execute("""
CREATE TABLE IF NOT EXISTS room_assignment (
    room_id TEXT,
    property_uuid TEXT,
    PRIMARY KEY (room_id, property_uuid),
    FOREIGN KEY (room_id) REFERENCES room(room_id),
    FOREIGN KEY (property_uuid) REFERENCES property(property_uuid)
);
""")

# Step 4: Populate property and room tables from cleaning_order
cur.execute("INSERT OR IGNORE INTO property (property_uuid) SELECT DISTINCT property_uuid FROM cleaning_order WHERE property_uuid IS NOT NULL;")
cur.execute("INSERT OR IGNORE INTO room (room_id) SELECT DISTINCT room FROM cleaning_order WHERE room IS NOT NULL;")

# Step 5: Populate room_assignment
cur.execute("""
INSERT OR IGNORE INTO room_assignment (room_id, property_uuid)
SELECT DISTINCT room, property_uuid
FROM cleaning_order
WHERE room IS NOT NULL AND property_uuid IS NOT NULL;
""")

# Step 6: (Optional) Add foreign keys to cleaning_order
# You can also keep this denormalized for simplicity if needed
# Uncomment below if you want strict enforcement

# cur.execute("PRAGMA foreign_keys = OFF;")  # Temporarily disable to allow table modification
# cur.execute("DROP TABLE IF EXISTS cleaning_order_normalized;")
# cur.execute("""
# CREATE TABLE cleaning_order_normalized AS
# SELECT co.*, r.room_id, p.property_uuid
# FROM cleaning_order co
# LEFT JOIN room_assignment ra ON co.room = ra.room_id AND co.property_uuid = ra.property_uuid
# LEFT JOIN room r ON co.room = r.room_id
# LEFT JOIN property p ON co.property_uuid = p.property_uuid;
# """)
# cur.execute("PRAGMA foreign_keys = ON;")

conn.commit()
conn.close()

print("✅ Normalization complete: room, property, and room_assignment tables created.")
