import psycopg2
import csv

# Database connection parameters
db_params = {
    'host': 'xx.xx.xx.xx',
    'port': 'xxxx',
    'database': 'sample_db',
    'user': 'enterprisedb',
    'password': 'xxxxxxxxxxx'
}

# Function to calculate the level of foreign key hierarchy
def calculate_fk_level(table_name, conn, visited_tables):
    if table_name in visited_tables:
        return 0
    visited_tables.add(table_name)

    cursor = conn.cursor()
    cursor.execute(f"SELECT conname, confrelid FROM pg_constraint WHERE confrelid = '{table_name}'::regclass")
    fk_tables = [row[1] for row in cursor.fetchall()]
    cursor.close()

    if not fk_tables:
        return 0

    max_level = 0
    for fk_table in fk_tables:
        level = calculate_fk_level(fk_table, conn, visited_tables)
        max_level = max(max_level, level)

    return max_level + 1

# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(**db_params)
except Exception as e:
    print("Error connecting to the database:", e)
    exit()

# Get a list of all tables in the public schema
cursor = conn.cursor()
cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
tables = [row[0] for row in cursor.fetchall()]
cursor.close()

# Prepare the CSV file
csv_filename = 'fk_hierarchy.csv'
with open(csv_filename, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['Table Name', 'FK Hierarchy Level'])

    for table_name in tables:
        visited_tables = set()
        fk_level = calculate_fk_level(table_name, conn, visited_tables)
        csv_writer.writerow([table_name, fk_level])

# Close the database connection
conn.close()

print(f"CSV report generated: {csv_filename}")
