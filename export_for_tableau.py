import sqlite3
import pandas as pd

print("Exporting database to CSVs for Tableau...")

# Connect to your local database
conn = sqlite3.connect('reconciliation_results.db')

# List of the tables and views we want to visualize
tables_to_export = [
    'matched_transactions', 
    'unmatched_bank', 
    'unmatched_wallet', 
    'v_executive_summary', 
    'v_daily_trends'
]

# Loop through and save each one as a CSV
for table in tables_to_export:
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    filename = f"tableau_{table}.csv"
    df.to_csv(filename, index=False)
    print(f" -> Created {filename}")

conn.close()
print("All done! Ready for Tableau.")