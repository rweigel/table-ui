
import logging
logger = logging.getLogger(__name__)

def write(name, header, body, file, types=None, metadata=None, logger=None, logger_indent="   "):
  import os
  import sqlite3

  indent = logger_indent

  if os.path.exists(file):
    logger.info(f"{indent}Removing existing SQLite database file '{file}'")
    os.remove(file)

  column_types = _types(header, types)

  header, body = _prep(header, body, column_types, logger, logger_indent="   ")

  for hidx, colname in enumerate(header):
    header[hidx] = f"`{colname}`"

  column_names = f"({', '.join(header)})"
  column_spec  = "(" + ", ".join(f"{col} {t}" for col, t in zip(header, column_types.values())) + ")"
  column_vals  = f"({', '.join(len(header)*['?'])})"

  execute = f'INSERT INTO `{name}` {column_names} VALUES {column_vals}'
  create  = f'CREATE TABLE `{name}` {column_spec}'

  logger.debug(f"{indent}Creating and connecting to file '{file}'")
  conn = sqlite3.connect(file)
  logger.debug(f"{indent}Done")

  logger.info(f"{indent}Getting cursor from connection to '{file}'")
  cursor = conn.cursor()
  logger.debug(f"{indent}Done")

  logger.debug(f"{indent}Creating table using cursor.execute('{create}')")
  cursor.execute(create)
  logger.debug(f"{indent}Done")

  logger.info(f"{indent}Inserting rows")
  logger.debug(f"{indent}using cursor.executemany('{execute}', body)")
  cursor.executemany(execute, body)
  logger.debug(f"{indent}Done")

  logger.debug(f"{indent}Executing: connection.commit()")
  conn.commit()
  logger.debug(f"{indent}Done")

  if header is not None:
    index = f"CREATE INDEX idx0 ON `{name}` ({header[0]})"
    logger.debug(f"{indent}Creating index using cursor.execute('{index}')")
    cursor.execute(index)
    logger.debug(f"{indent}Done")

    logger.debug(f"{indent}Executing: commit()")
    conn.commit()
    logger.debug(f"{indent}Done")

  conn.close()

  if metadata is None:
    return

  conn = sqlite3.connect(file)
  cursor = conn.cursor()
  name_desc = f'{name}.metadata'
  logger.info(f"{indent}Creating table {name_desc} with table metadata stored as a JSON string")

  spec = "(TableName TEXT NOT NULL, Metadata TEXT)"
  execute = f"CREATE TABLE `{name_desc}` {spec}"
  logger.debug(f"{indent}Executing: {execute}")
  conn.execute(execute)
  logger.debug(f"{indent}Done")

  import json
  metadata = json.dumps(metadata)
  metadata = metadata.replace("'","''")
  values = f"('{name_desc}', '{metadata}')"
  insert = f'INSERT INTO `{name_desc}` ("TableName", "Metadata") VALUES {values}'
  logger.debug(f"{indent}Executing: connection.execute('{insert})'")
  conn.execute(insert)
  logger.debug(f"{indent}Done.")
  conn.commit()
  conn.close()


def _types(columns, types):
  # Build column type map: TEXT by default
  valid_types = {'TEXT', 'INTEGER', 'REAL', 'NUMERIC', 'BLOB'}
  type_map = {col: 'TEXT' for col in columns}
  if types is not None:
    if isinstance(types, list):
      if len(types) != len(columns):
        raise ValueError(f"types list length ({len(types)}) does not match number of columns ({len(columns)}). Exiting.")
      for col, t in zip(columns, types):
        t_upper = t.upper()
        if t_upper not in valid_types:
          raise ValueError(f"Invalid type '{t}' for column '{col}'. Must be one of {valid_types}.")
        type_map[col] = t_upper
    elif isinstance(types, dict):
      for col, t in types.items():
        if col not in type_map:
          raise ValueError(f"types dict key '{col}' not found in columns. Exiting.")
        t_upper = t.upper()
        if t_upper not in valid_types:
          raise ValueError(f"Invalid type '{t}' for column '{col}'. Must be one of {valid_types}.")
        type_map[col] = t_upper
    else:
      raise ValueError("types must be a list or dict. Exiting.")
  return type_map


def _prep(header, body, types, logger, logger_indent="   "):
  import time

  indent = logger_indent

  def unique(header):

    headerlc = [val.lower() for val in header]
    headeru = header.copy()
    for val in header:
      indices = [i for i, x in enumerate(headerlc) if x == val.lower()]
      if len(indices) > 1:
        dups = [header[i] for i in indices]
        logger.warning(f"{indent}Duplicate column names when cast to lower case: {str(dups)}.")
        logger.warning(f"{indent}Renaming duplicates by appending _$DUPLICATE_NUMBER$ to the column name.")
        for r, idx in enumerate(indices):
          if r > 0:
            newname = header[idx] + "_$" + str(r) + "$"
            logger.info(f"{indent}Renaming {header[idx]} to {newname}")
            headeru[idx] = newname
    return headeru


  logger.info(f"{indent}Renaming non-unique column names")
  header = unique(header)
  logger.info(f"{indent}Renamed non-unique column names")

  type_cast = {
    'TEXT':    str,
    'INTEGER': int,
    'REAL':    float,
    'NUMERIC': float,
    'BLOB':    lambda x: x,
  }
  col_types_list = list(types.values()) if types else []

  logger.info(f"{indent}Casting table elements using column types.")
  start = time.time()
  for i, row in enumerate(body):
    for j, val in enumerate(row):
      col_type = col_types_list[j] if j < len(col_types_list) else 'TEXT'
      cast_fn = type_cast.get(col_type, str)
      try:
        body[i][j] = cast_fn(val)
      except (ValueError, TypeError):
        body[i][j] = str(val)

  dt = "{:.2f} [s]".format(time.time() - start)
  logger.info(f"{indent}Cast table elements in {len(body)} rows and {len(header)} columns in {dt}")

  return header, body


def _cursor(sqldb, memory=False):
  import os
  import sqlite3
  import threading
  # memory = True not well tested.
  #  Seems much slower for small and fast queries and
  #  ~3x faster for large and complex queries.
  if not memory:
    connection = sqlite3.connect(sqldb)
    cursor = connection.cursor()
  else:
    try:
      # Open the file-based DB and a fresh in-memory DB
      file_conn = sqlite3.connect(sqldb)
      mem_conn = sqlite3.connect(":memory:")
      # Copy the whole file DB into memory
      # Simple module-level cache to avoid re-copying unchanged DB files into memory
      if '_MEMORY_DB_CACHE' not in globals():
        _MEMORY_DB_CACHE = {}
        _MEMORY_DB_LOCK = threading.Lock()
      else:
        _MEMORY_DB_CACHE = globals()['_MEMORY_DB_CACHE']
        _MEMORY_DB_LOCK = globals()['_MEMORY_DB_LOCK']

      file_path = os.path.abspath(sqldb)
      try:
        mtime = os.path.getmtime(file_path)
      except Exception:
        mtime = None

      # If we've already copied this exact file (same mtime), reuse the in-memory connection
      with _MEMORY_DB_LOCK:
        cache_entry = _MEMORY_DB_CACHE.get(file_path)
        if cache_entry is not None and cache_entry[0] == mtime:
          connection = cache_entry[1]
          cursor = connection.cursor()
          return cursor, connection

      # Otherwise create a fresh in-memory DB (closing any stale cached connection)
      if cache_entry is not None:
        try:
          cache_entry[1].close()
        except Exception:
          pass
        with _MEMORY_DB_LOCK:
          _MEMORY_DB_CACHE.pop(file_path, None)

      try:
        # Open the file-based DB and a fresh in-memory DB, then copy
        logger.info(f"Copying database '{sqldb}' into memory")
        file_conn = sqlite3.connect(sqldb)
        mem_conn = sqlite3.connect(":memory:")
        file_conn.backup(mem_conn)
        file_conn.close()
        connection = mem_conn
        cursor = mem_conn.cursor()
        # Cache the new in-memory DB keyed by absolute path and current mtime
        with _MEMORY_DB_LOCK:
          _MEMORY_DB_CACHE[file_path] = (mtime, mem_conn)
      except Exception:
        # Ensure any opened connections are closed on error
        try:
          file_conn.close()
        except Exception:
          pass
        try:
          mem_conn.close()
        except Exception:
          pass
        raise
    except Exception:
      # Ensure any opened connections are closed on error
      try:
        file_conn.close()
      except Exception:
        pass
      try:
        mem_conn.close()
      except Exception:
        pass
      raise

  return cursor, connection


def execute(sqldb, query, params=None):

  import time

  start = time.time()
  cursor, connection = _cursor(sqldb, memory=False)

  logger.info("  Executing")
  logger.info(f"  {query}")
  if params:
      logger.info("  with parameters:")
      logger.info(f"  {params}")
  logger.info("  and fetching all results from")
  logger.info(f"  {sqldb if sqldb is not None else 'existing connection'}")
  if params:
      result = cursor.execute(query, params)
  else:
      result = cursor.execute(query)
  data = result.fetchall()
  connection.close()
  dt = "{:.4f} [s]".format(time.time() - start)
  n_rows = len(data)
  n_cols = len(data[0]) if n_rows > 0 else 0
  logger.info(f"  {dt} to execute query and fetch {n_rows}x{n_cols} table.")

  return data


def table_names(sqldb):

  query = "SELECT name FROM sqlite_master WHERE type='table';"
  data = execute(sqldb, query)
  return [row[0] for row in data]


def column_names(sqldb, table_name):
  query = f"PRAGMA table_info(`{table_name}`);"
  data = execute(sqldb, query)
  return [row[1] for row in data] if data else []


def uniques(sqldb, table_name, column_name, clause=None, params=None):
  clause = clause if clause else ""
  query = f"SELECT `{column_name}`, COUNT(*) as count FROM `{table_name}` "
  query += f"{clause} GROUP BY `{column_name}`"
  data = execute(sqldb, query, params=params)
  return [(row[0], row[1]) for row in data] if data else []


def nrows(sqldb, table_name, clause=None, params=None):
  clause_str = clause if clause else ""
  query = f"SELECT COUNT(*) FROM `{table_name}` {clause_str}"
  data = execute(sqldb, query, params=params)
  return data[0][0] if data else 0