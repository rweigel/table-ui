import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def json2sql(json_head, json_body, sqldb=None):
  import os
  import json
  import sqlite3

  if sqldb is None:
    if json_body.endswith(".json"):
      sqldb_path = json_body.replace(".json", ".sqlite")
    else:
      sqldb_path = json_body + ".sqlite"

  # Read header and body
  with open(json_head) as f:
    columns = json.load(f)
  with open(json_body) as f:
    rows = json.load(f)

  # Remove old sqldb if exists
  if os.path.exists(sqldb_path):
    os.remove(sqldb_path)

  # Create table
  conn = sqlite3.connect(sqldb_path)
  cursor = conn.cursor()
  col_defs = ", ".join([f'"{col}" TEXT' for col in columns])
  cursor.execute(f'CREATE TABLE demo ({col_defs})')

  # Insert rows
  for row in rows:
      placeholders = ", ".join(["?"] * len(row))
      cursor.execute(f'INSERT INTO demo VALUES ({placeholders})', row)

  conn.commit()

  # Verify table creation
  cursor.execute("SELECT * FROM demo")
  all_rows = cursor.fetchall()

  logger.debug(f"Inserted {len(all_rows)} rows into demo table.")
  logger.debug(f"First row: {all_rows[0]}")

  conn.close()

  return sqldb_path
