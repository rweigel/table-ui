import utilrsw

logger = None

def dict2sql(datasets, config, name, out_dir='.', embed=False, logger=None):

  if logger is None:
    logger = utilrsw.logger('dict2sql')
  globals()['logger'] = logger

  attributes = {}
  paths = config['paths']
  for path in paths:
    attributes[path] = config['paths'][path]

  path_type = config.get('path_type', 'dict')
  if path_type == 'list':
    if len(paths) != 1:
      emsg = "Error: If path_type = 'list', only one path may be given."
      raise Exception(emsg)
    path = list(paths.keys())[0]
    attributes[path] = utilrsw.flatten_dicts(paths[path], simplify=True)
    paths[path] = utilrsw.flatten_dicts(paths[path], simplify=True)

  attribute_counts = None
  if config.get('use_all_attributes', False):
    # Modify attributes dict to include all unique attributes found in all
    # variables. If an attribute is misspelled, it is mapped to the correct
    # spelling and placed in the attributes dict if there is a fixes for in
    # name config.json. The return value of attributes_all is a list 
    # of all uncorrected attribute names encountered.
    import collections
    logger.info("Finding all unique attributes")
    attributes_all = _table_walk(datasets, attributes, config, path_type, mode='attributes')
    attribute_counts = collections.Counter(attributes_all)
    attribute_counts = sorted(attribute_counts.items(), key=lambda i: i[0].lower())

    if attributes_all is None:
      logger.error("No attributes found")
      raise Exception("No attributes found")
    else:
      s = "" if len(attributes_all) == 1 else "s"
      logger.info(f"Found {len(attributes_all)} attribute{s}")

  # Create table header based on attributes dict.
  header = _table_header(attributes, config.get('id_name', None))

  logger.info("Creating table rows")
  table = _table_walk(datasets, attributes, config, path_type, mode='rows')
  s = "" if len(table) == 1 else "s"
  logger.info(f"Created {len(table)} table row{s}")

  if len(table) > 0 and len(header) != len(table[0]):
    emsg = f"len(header) == {len(header)} != len(table[0]) = {len(table[0])}"
    raise Exception(emsg)

  if len(table) == 0:
    raise Exception(f"No rows in {name} table for id='{id}'")

  info = _write_files(name, config, out_dir, header, table, attribute_counts)

  if embed:
    # Return header, table, in place of file paths.
    info['header_data'] = header
    info['body_data'] = table
    if attribute_counts is not None:
      info['counts_data'] = {}
      for count in attribute_counts:
        info['counts_data'][count[0]] = count[1]

  return info

def _table_header(attributes, id_name):

  header = []
  for path in attributes.keys():
    for attribute in attributes[path]:
      header.append(attribute)

  return header


def _table_walk(datasets, attributes, config, path_type, mode='attributes'):
  """
  If mode='attributes', returns a dictionary of attributes found across all
  datasets and paths starting with the given attributes. If the attribute
  is misspelled, it is mapped to the correct spelling.

  If mode='rows', returns a list of rows for the table. Each row contains the
  value of the associated attribute (accounting for misspellings) in the given
  attributes dictionary. If the path does not have the attribute, an empty
  string is used for the associated column.
  """
  import copy

  assert mode in ['attributes', 'rows']

  omit_attributes = config.get('omit_attributes', None)

  fixes = None
  if 'fix_attributes' in config:
    if config['fix_attributes']:
      if 'fixes' in config:
        if mode == 'attributes':
          logger.info("Using fixes found in config")
        fixes = config['fixes']
      else:
        msg = "Error: 'fix_attributes' = True, but 'fixes' in config."
        logger.error(msg)

  if mode == 'attributes':
    attribute_names = []
  else:
    table = []
    row = []

  n_cols_last = None
  datasets = copy.deepcopy(datasets)

  paths = attributes.keys()

  for id, dataset in datasets.items():
    logger.info(f"  Computing {mode} for id = '{id}'")

    if path_type == 'dict':

      if mode == 'rows':
        row = []

      for path in paths:

        logger.info(f"    Reading path = '{path}'")

        data = utilrsw.get_path(dataset, path.split('/'))

        if data is None:
          if mode == 'rows':
            logger.info(f"    Path '{path}' not found. Inserting '?' for all attribute values.")
            # Insert "?" for all attributes
            n_attribs = len(attributes[path])
            fill = n_attribs*"?".split()
            row = [*row, *fill]
          continue

        if mode == 'attributes':
          _add_attributes(data, attributes[path], attribute_names, fixes, id + "/" + path, omit_attributes)
        else:
          _append_columns(data, attributes[path], row, fixes, omit_attributes)

      if mode == 'rows':
        logger.debug(f"  {len(row)} columns in row {len(table)}")
        if n_cols_last is not None and len(row) != n_cols_last:
          loc = id + "/" + path
          emsg = f"In {loc}, number of columns changed from"
          emsg += f"{n_cols_last} to {len(row)}."
          raise Exception(emsg)
        n_cols_last = len(row)
        table.append(row)

    else:

      # Get first (only) path
      path = next(iter(paths))
      data = utilrsw.get_path(dataset, path.split('/'))

      if data is None:
        logger.debug(f"  Path '{path}' not found in dataset '{id}'.")
        continue

      for variable in data:

        if mode == 'rows':
          row = []

        if mode == 'attributes':
          _add_attributes(variable, attributes[path], attribute_names, fixes, id + "/" + path, omit_attributes)
        else:
          _append_columns(variable, attributes[path], row, fixes, omit_attributes)

        # Add row for variable
        if mode == 'rows':
          logger.debug(f"  row #{len(table)}: {len(row)} columns")
          if n_cols_last is not None and len(row) != n_cols_last:
            loc = id + "/" + path
            emsg = f"In {loc}, number of columns changed from"
            emsg += f"{n_cols_last} to {len(row)}."
            raise Exception(emsg)
          n_cols_last = len(row)
          table.append(row)

  if mode == 'attributes':
    return attribute_names
  else:
    return table

def _append_columns(data, attributes, row, fixes, omit_attributes):

  for attribute in attributes:

    if omit_attributes is not None and attribute in omit_attributes:
      logger.info(f"  Skipping {attribute}")
      continue

    if fixes is not None:
      for fix in fixes:
        if fix in data:
          data[fixes[fix]] = data[fix]
          del data[fix]

    if attribute in data:
      val = data[attribute]
      if isinstance(val, str) and val == " ":
        val = val.replace(' ', '⎵')
      row.append(val)
    else:
      row.append("")

def _add_attributes(data, attributes, attribute_names, fixes, path, omit_attributes):

  for attribute_name in data:

    if omit_attributes is not None and attribute_name in omit_attributes:
      logger.info(f"  Skipping {attribute_name}")
      continue

    attribute_names.append(attribute_name)
    if fixes is None or attribute_name not in fixes:
      attributes[attribute_name] = None
    else:
      wmsg = f"  Fixing attribute name: {path}/{attribute_name}"
      logger.warning(f"{wmsg} -> {fixes[attribute_name]}")
      attributes[fixes[attribute_name]] = None


def _write_files(name, config, out_dir, header, body, counts):
  import os

  files = {
    'meta': f'{name}.meta.json',
    'header': f'{name}.head.json',
    'body': f'{name}.body.json',
    'csv': f'{name}.csv',
    'sql': f'{name}.sql',
    'counts': f'{name}.attribute_counts.csv'
  }

  metadata = _table_metadata(name, config, header, files)

  for key in files:
    files[key] = os.path.join(out_dir, files[key])

  if counts is None:
    del files['counts']
  else:
    logger.info(f"Writing {files['counts']}")
    utilrsw.write(files['counts'], [["attribute", "count"], *counts])

  logger.info(f"Writing: {files['meta']}")
  utilrsw.write(files['meta'], metadata)

  logger.info(f"Writing: {files['header']}")
  utilrsw.write(files['header'], header)

  logger.info(f"Writing: {files['body']}")
  utilrsw.write(files['body'], body)

  logger.info(f"Writing: {files['csv']}")
  utilrsw.write(files['csv'], [header, *body])

  logger.info(f"Writing: {files['sql']}")
  _sql_write(name, header, body, f"{files['sql']}", metadata)

  return files

def _table_metadata(name, config, header, files):
  import datetime

  creationDate = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
  columnDefinitions = config.get('column_definitions', {})
  table_metadata = {
    "description": config.get('description', ""),
    "creationDate": creationDate,
    "columnDefinitions": columnDefinitions
  }

  for column_name in header:
    if column_name not in columnDefinitions:
      #logger.warning(f"{indent}Column name '{column_name}' not in column_definitions for table '{name}'")
      table_metadata["columnDefinitions"][column_name] = None

  return table_metadata

def _sql_write(name, header, body, file, metadata):
  import os

  indent = "   "
  import sqlite3

  if os.path.exists(file):
    logger.info(f"{indent}Removing existing SQLite database file '{file}'")
    os.remove(file)

  header, body = _sql_prep(header, body)

  for hidx, colname in enumerate(header):
    header[hidx] = f"`{colname}`"

  column_names = f"({', '.join(header)})"
  column_spec  = f"({', '.join(header)} TEXT)"
  column_vals  = f"({', '.join(len(header)*['?'])})"

  create  = f'CREATE TABLE `{name}` {column_spec}'
  execute = f'INSERT INTO `{name}` {column_names} VALUES {column_vals}'

  logger.debug(f"{indent}Creating and connecting to file '{file}'")
  conn = sqlite3.connect(file)
  logger.debug(f"{indent}Done")

  logger.info(f"{indent}Getting cursor from connection to '{file}'")
  cursor = conn.cursor()
  logger.debug(f"{indent}Done")

  logger.debug(f"{indent}Creating index using cursor.execute('{create}')")
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

def _sql_prep(header, body):
  import time

  indent = "   "

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

  logger.info(f"{indent}Casting table elements to str.")
  start = time.time()
  for i, row in enumerate(body):
    for j, _ in enumerate(row):
      body[i][j] = str(body[i][j])

  dt = "{:.2f} [s]".format(time.time() - start)
  logger.info(f"{indent}Casted table elements to str in {len(body)} rows and {len(header)} columns in {dt}")

  return header, body
