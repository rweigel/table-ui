def list2sql(table_name, json_body, json_head=None, out=None):
  import os
  import json
  import logging
  import sqlite3

  logger = logging.getLogger(__name__)
  logging.basicConfig(level=logging.INFO)

  out_path = out
  if out is None:
    if json_body.endswith(".json"):
      out_path = json_body.replace(".json", ".sqlite")
    else:
      out_path = json_body + ".sqlite"

  with open(json_body) as f:
    rows = json.load(f)
    if not isinstance(rows, list):
      raise ValueError(f"{json_body} must contain an array of arrays. Exiting.")
      exit(1)
    if len(rows) == 0:
      raise ValueError(f"{json_body} contains no rows. Exiting.")
      exit(1)

  if json_head is None:
    if len(rows) > 0:
      num_cols = len(rows[0])
    columns = [f"c{i}" for i in range(num_cols)]
  else:
    with open(json_head) as f:
      columns = json.load(f)
    if not isinstance(columns, list):
      raise ValueError(f"{json_head} must contain an array of strings. Exiting.")
      exit(1)
    if len(columns) != len(rows[0]):
      raise ValueError(f"Number of values in {json_head} ({len(columns)}) does not match number elements in first row of {json_body} ({len(rows[0])}). Exiting.")
      exit(1)

  # Remove old out if exists
  if os.path.exists(out_path):
    os.remove(out_path)

  # Create table
  conn = sqlite3.connect(out_path)
  cursor = conn.cursor()
  col_defs = ", ".join([f'`{col}` TEXT' for col in columns])
  cursor.execute(f'CREATE TABLE {table_name} ({col_defs})')

  # Insert rows
  cursor.executemany(f'INSERT INTO {table_name} VALUES ({", ".join(["?"] * len(columns))})', rows)
  conn.commit()
  logger.debug(f"Wrote {len(columns)} columns and {len(rows)} rows to {out_path}")

  # Verify table creation
  cursor.execute(f"SELECT * FROM {table_name}")
  all_rows = cursor.fetchall()

  assert len(all_rows) == len(rows)

  conn.close()

  return out_path
