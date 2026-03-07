def list2sql(table_name, body, head=None, types=None, out=None):
  import json
  import logging

  import tableui

  logger = logging.getLogger(__name__)
  logging.basicConfig(level=logging.INFO)

  if isinstance(body, str):
    with open(body) as f:
      rows = json.load(f)
      if not isinstance(rows, list):
        raise ValueError(f"{body} must contain an array of arrays. Exiting.")
        exit(1)
      if len(rows) == 0:
        raise ValueError(f"{body} contains no rows. Exiting.")
        exit(1)
  else:
    rows = body
    if not isinstance(rows, list):
      raise ValueError("body must be a list of lists. Exiting.")
      exit(1)
    if len(rows) == 0:
      raise ValueError("body contains no rows. Exiting.")
      exit(1)

  if not all(isinstance(row, list) for row in rows):
    raise ValueError("body must contain an array of arrays. Exiting.")
    exit(1)

  if head is None:
    if len(rows) > 0:
      num_cols = len(rows[0])
    columns = [f"c{i}" for i in range(num_cols)]
  else:
    if isinstance(head, str):
      with open(head) as f:
        columns = json.load(f)
      if not isinstance(columns, list):
        raise ValueError(f"{head} must contain an array of strings. Exiting.")
        exit(1)
    else:
      columns = head

  if len(columns) != len(rows[0]):
    raise ValueError(f"Number of values in {head} ({len(columns)}) does not match number elements in first row of {body} ({len(rows[0])}). Exiting.")
    exit(1)

  out_path = out
  if out is None:
    if isinstance(body, str):
      if body.endswith(".json"):
        out_path = body.replace(".json", ".sqlite")
      else:
        out_path = body + ".sqlite"
    else:
      out_path = "out.sqlite"

  tableui.sql.write(table_name, columns, rows, out_path, types=types, logger=logger, logger_indent="   ")

  return out_path
