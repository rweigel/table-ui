import os
import json
import logging
import time
import urllib

import sqlite3
import uvicorn
import fastapi

import tableui

logger = logging.getLogger(__name__)

root_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
def serve(table_name=None, sqldb=None,
          json_head=None, json_body=None,
          host="0.0.0.0", port=5001, root_dir=root_dir, debug=False,
          config="config.json", render="js/render.js"):

  if debug:
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.INFO)

  # Set defaults using kwargs (from locals()) and cli values
  kwargs = tableui.cli.defaults(locals())

  dbconfig = {
    "sqldb": kwargs['sqldb'],
    "table_name": kwargs['table_name'],
    "json_head": kwargs['json_head'],
    "json_body": kwargs['json_body']
  }

  apiconfig = {
    "root_dir": kwargs['root_dir'],
    "dtrender": kwargs['render'],
    "dtconfig": kwargs['config'],
    "dbconfig": dbconfig
  }

  runconfig = {
                "host": kwargs['host'],
                "port": kwargs['port'],
                "server_header": False
              }

  for file in [sqldb, json_head, json_body, config, render]:
    if file is None:
      continue
    if not os.path.exists(file):
      logger.error(f"File not found: {file}. Exiting.")
      exit(1)

  _dbinfo(**dbconfig)  # Test if dbconfig is valid

  logger.info("Initalizing API")
  app = fastapi.FastAPI()
  _api_init(app, apiconfig)

  logger.info("Starting server")
  uvicorn.run(app, **runconfig)


def _read_config(apiconfig, warn=False):

  if not os.path.exists(apiconfig['dtconfig']):
    logger.error("Error: Config file not found: " + apiconfig['dtconfig'])
    exit(1)

  with open(apiconfig['dtconfig']) as f:
    logger.info("Reading: " + apiconfig['dtconfig'])
    content = json.load(f)

  if 'jsondb' in apiconfig['dbconfig'] and 'sqldb' in apiconfig['dbconfig']:
    logger.error("Error: Both sqldb and jsondb were given. Choose one. Exiting.")
    exit(1)

  serverSide = content.get('serverSide', None)

  if serverSide is None:
    if apiconfig['dbconfig']['sqldb'] is not None:
      content['serverSide'] = True
    if apiconfig['dbconfig']['json_body'] is not None:
      content['serverSide'] = False
  else:
    if serverSide not in [True, False]:
      logger.error("Error: Config file 'serverSide' must be true or false")
      exit(1)
    if apiconfig['dbconfig']['sqldb'] is not None and not serverSide:
      if warn:
        logger.warning("Warning: Config file specifies serverSide=false but input is sqldb. Overriding to serverSide=true")
      content['serverSide'] = True
    if apiconfig['dbconfig']['json_body'] and serverSide:
      if warn:
        logger.warning("Warning: Config file specifies serverSide=true but jsondb was given. Overriding to serverSide=false")
      content['serverSide'] = False

  return content


def _api_init(app, apiconfig):
  # Must import StaticFiles from fastapi.staticfiles
  # fastapi.staticfiles is not in dir(fastapi) (it is added dynamically)
  from fastapi.staticfiles import StaticFiles

  def cors_headers(response: fastapi.Response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, HEAD, OPTIONS"
    return response

  root_dir = apiconfig['root_dir']
  dtrender = apiconfig['dtrender']
  dbconfig = apiconfig['dbconfig']

  for dir in ['js', 'css', 'img', 'demo', 'misc']:
    directory = os.path.join(root_dir, dir)
    app.mount(f"/{dir}/", StaticFiles(directory=directory))

  @app.route("/", methods=["GET", "HEAD"])
  def indexhtml(request: fastapi.Request):
    # Silently ignores any query parameters
    fname = os.path.join(root_dir,'index.html')
    logger.info("Reading: " + fname)
    with open(fname) as f:
      indexhtml_ = f.read()
    return fastapi.responses.HTMLResponse(indexhtml_)

  dbinfo = _dbinfo(**dbconfig)
  if "jsondb" in dbinfo and dbinfo["jsondb"] is not None:
    @app.route("/jsondb", methods=["GET", "HEAD"])
    def jsondb(request: fastapi.Request):
      # Silently ignores any query parameters other than _verbose
      query_params = dict(request.query_params)
      dbinfo = _dbinfo(**dbconfig)
      with open(dbinfo['jsondb']['body']) as f:
        # TODO: Stream
        logger.info("Reading and sending: " + dbinfo['jsondb']['body'])
        data = json.load(f)
        # TODO: Use _verbose validation in data()
        data = _data_transform(data, dbinfo['column_names'], query_params.get("_verbose", None) == "true")
      data = {
        "columns": dbinfo['column_names'],
        "data": data
      }
      return fastapi.responses.JSONResponse(data)

  if "sqldb" in dbinfo and dbinfo["sqldb"] is not None:
    @app.route("/sqldb", methods=["GET", "HEAD"])
    def sqldb(request: fastapi.Request):
      # Silently ignores any query parameters
      filename = os.path.basename(dbinfo['sqldb'])
      if filename.endswith('.sqlite'):
        filename = filename[0:-7] + '.sqlite3'
      if filename.endswith('.sql'):
        filename = filename[0:-4] + '.sqlite3'
        kwargs = {
                    'media_type': 'application/x-sqlite3',
                    'filename': filename
                  }
      return fastapi.responses.FileResponse(dbinfo['sqldb'], **kwargs)

  @app.route("/config", methods=["GET", "HEAD"])
  def config(request: fastapi.Request):
    # Silently ignores any query parameters
    dbinfo = _dbinfo(**dbconfig)
    content = _read_config(apiconfig, warn=False)
    if "tableUI" not in content:
      content['tableUI'] = {"tableMetadata": {}}
    if "sqldb" in dbinfo:
      content['tableUI']['sqldb'] = dbinfo["sqldb"]
    if "jsondb" in dbinfo:
      content['tableUI']['jsondb'] = dbinfo["jsondb"]
    if "table_name" in dbinfo and 'name' not in content['tableUI']['tableMetadata']:
      content['tableUI']['tableMetadata']['name'] = dbinfo["table_name"]
    try:
      import datetime
      mtime = os.path.getmtime(dbinfo.get("sqldb", dbinfo.get("jsondb", {}).get("body", "")))
      content['tableUI']['tableMetadata']['creationDate'] = datetime.datetime.fromtimestamp(mtime).isoformat()
    except Exception as e:
      logger.warning(f"Could not get file modification time: {e}")
    return fastapi.responses.JSONResponse(content=content)

  @app.route("/render.js", methods=["GET", "HEAD"])
  def render(request: fastapi.Request):
    # Silently ignores any query parameters
    return fastapi.responses.FileResponse(dtrender)

  @app.route("/header", methods=["GET", "HEAD"])
  def header(request: fastapi.Request):
    # Silently ignores any query parameters
    dbinfo = _dbinfo(**dbconfig)
    return fastapi.responses.JSONResponse(content=dbinfo['column_names'])

  @app.route("/data/", methods=["POST", "GET", "HEAD"])
  def data(request: fastapi.Request):

    logger.info(f"Received data request with query params: {request.query_params}")
    query_params = dict(request.query_params)

    dbinfo = _dbinfo(**dbconfig)

    if "_verbose" in query_params:
      if query_params["_verbose"] not in ["true", "false"]:
        emsg = "Error: _verbose must be 'true' or 'false'"
        return fastapi.responses.JSONResponse(content={"error": emsg}, status_code=400)
      if query_params["_verbose"] == "true":
        query_params["_verbose"] = True
      else:
        query_params["_verbose"] = False
    else:
      query_params["_verbose"] = False

    if "jsondb" in dbinfo:
      # No server-side processing. Serve entire JSON and return.
      for key in query_params.keys():
        if key not in ["_", "_verbose"]:
          emsg = f"Error: Unknown query parameter: {key}. Only '_' and '_verbose' allowed when using jsondb (=> serverSide=false)."
          return fastapi.responses.JSONResponse(content={"error": emsg}, status_code=400)

      with open(dbinfo['jsondb']['body']) as f:
        # TODO: Stream
        logger.info("Reading and sending: " + dbinfo['jsondb']['body'])
        data = json.load(f)
        data = _data_transform(data, dbinfo['column_names'], query_params["_verbose"])
        return fastapi.responses.JSONResponse({"data": data})

    # sqldb and server-side processing
    keys_allowed = [
      '_',
      '_draw',
      '_start',
      '_length',
      '_orders',
      '_return',
      '_uniques',
      '_verbose'
    ]

    column_names = dbinfo['column_names']
    for key in query_params.keys():
      if key not in keys_allowed and key not in column_names:
        logger.error(f"Error: Unknown query parameter: {key}.")
        emsg = f"Error: Unknown query parameter with first five character of {key[0:5]}. Allowed: {keys_allowed} and "
        emsg += f"column names: {column_names}"
        return fastapi.responses.JSONResponse(content={"error": emsg}, status_code=400)

    if "_uniques" in query_params:
      if query_params["_uniques"] not in ["true", "false"]:
        emsg = "Error: _uniques must be 'true' or 'false'"
        return fastapi.responses.JSONResponse(content={"error": emsg}, status_code=400)
      if query_params["_uniques"] == "true":
        query_params["_uniques"] = True
      else:
        query_params["_uniques"] = False
    else:
      query_params["_uniques"] = False

    def is_positive_integer(s):
      return s.isdigit() and int(s) >= 0

    if "_start" in query_params:
      if not is_positive_integer(query_params["_start"]):
        emsg = "Error: _start >= 0 required"
        return fastapi.responses.JSONResponse(content={"error": emsg}, status_code=400)
      query_params["_start"] = int(query_params["_start"])
    else:
      query_params["_start"] = 0

    if "_length" in query_params:
      _length = query_params["_length"]
      if not is_positive_integer(_length) or int(_length) == 0:
        emsg = "Error: _length > 0 required"
        return fastapi.responses.JSONResponse(content={"error": emsg}, status_code=400)
      query_params["_length"] = int(_length)
    else:
      query_params["_length"] = None

    if "_orders" in query_params:
      orders = query_params["_orders"].split(",")
      for order in orders:
        col = order
        if order.startswith("-"):
          col = order[1:]
        if col not in dbinfo['column_names']:
          emsg = f"Error: _orders column '{col}' not found in column names: "
          emsg += "{dbinfo['column_names']}"
          return fastapi.responses.JSONResponse(content={"error": emsg}, status_code=400)
      query_params["_orders"] = orders
    else:
      query_params["_orders"] = None

    searches = {}
    if query_params is not None:
      logger.info(f"Query params: {query_params}")
      for key, _ in query_params.items():
        if key in dbinfo['column_names']:
          kwargs = {'encoding': 'utf-8', 'errors': 'replace'}
          searches[key] = urllib.parse.unquote(query_params[key], **kwargs)
      query_params['searches'] = searches
    else:
      query_params['searches'] = None

    return_cols = dbinfo['column_names']
    if "_return" in query_params:
      query_params["_return"] = query_params["_return"].split(",")
      for col in query_params["_return"]:
        if col not in dbinfo['column_names']:
          emsg = f"Error: _return column '{col}' not found in column names: "
          emsg += "{dbinfo['column_names']}"
          return fastapi.responses.JSONResponse(content={"error": emsg}, status_code=400)
      return_cols = query_params["_return"]
    else:
      query_params["_return"] = None

    result = _dbquery(dbinfo, query_params)

    if query_params['_uniques']:
      length = query_params['_length']
      if length:
        for col in result['data'].keys():
          # Sort by the second element (count) in each (value, count) tuple
          result['data'][col] = sorted(result['data'][col], key=lambda x: -x[1])
          result['data'][col] = result['data'][col][0:length]
      return fastapi.responses.JSONResponse(content=result['data'])

    data = _data_transform(result['data'], return_cols, query_params["_verbose"])

    content = {
                "draw": int(query_params.get("_draw", 1)),
                "recordsTotal": result['recordsTotal'],
                "recordsFiltered": result['recordsFiltered'],
                "data": data
              }

    return fastapi.responses.JSONResponse(content=content)


def _dbquery(dbinfo, query_params):

  def execute(cursor, query):
    start = time.time()
    logger.info(f"Executing {query} and fetching all results")
    result = cursor.execute(query)
    data = result.fetchall()
    dt = "{:.4f} [s]".format(time.time() - start)
    logger.info(f"Took {dt} to execute query and fetch")
    return data

  def n_rows_filtered(cursor, clause):
    logger.info("Counting # of rows after applying search filters")
    query = f"SELECT COUNT(*) FROM `{dbinfo['table_name']}` {clause}"
    return execute(cursor, query)[0][0]

  def orderby(orders):
    if orders is None:
      return ""
    orderstr = "ORDER BY "
    for order in orders:
      if order.startswith("-"):
        orderstr += f"`{order[1:]}` DESC, "
      else:
        orderstr += f"`{order}` ASC, "
    orderstr = orderstr[:-2]
    return orderstr

  def clause(searches):
    if searches is None:
      return ""
    keys = list(searches.keys())
    where = []
    escape = "\\"
    for key in keys:
      if searches[key] == "''" or searches[key] == '""':
        where.append(f" `{key}` = ''")
      elif searches[key].startswith("'") and searches[key].endswith("'"):
        where.append(f" `{key}` = {searches[key]}")
      elif searches[key].startswith('%') and not searches[key].endswith('%'):
        where.append(f" `{key}` LIKE '{searches[key]}' ESCAPE '{escape}'")
      elif not searches[key].startswith('%') and searches[key].endswith('%'):
        where.append(f" `{key}` LIKE '{searches[key]}' ESCAPE '{escape}'")
      else:
        where.append(f" `{key}` LIKE '%{searches[key]}%' ESCAPE '{escape}'")
    if len(where) == 0:
      return ""
    return "WHERE" + " AND ".join(where)

  offset = query_params['_start']
  limit = query_params['_length']
  orders = query_params['_orders']
  searches = query_params['searches']
  _return = query_params['_return']
  uniques = query_params['_uniques']

  logger.info("Connecting to database file " + dbinfo['sqldb'])
  conn = sqlite3.connect(dbinfo['sqldb'])
  cursor = conn.cursor()

  recordsTotal = dbinfo['n_rows']
  recordsFiltered = recordsTotal

  if uniques:
    uniques = {}
    columns = _return
    if _return is None:
      columns = dbinfo['column_names']
    for col in columns:
      query = f"SELECT `{col}`, COUNT(*) as count FROM `{dbinfo['table_name']}` {clause(searches)} GROUP BY `{col}`"
      rows = execute(cursor, query)
      # Each value is a list of (value, count) tuples
      uniques[col] = rows.copy()
    return {"data": uniques}

  if _return is None:
    columns_str = "*"
  else:
    columns_str = ", ".join([f"`{col}`" for col in _return])

  query = f"SELECT {columns_str} FROM `{dbinfo['table_name']}` {clause(searches)} {orderby(orders)}"
  if offset == 0 and limit is None:
    logger.info("Executing query with no limit and offset")
    data = execute(cursor, query)
    if searches is not None:
      recordsFiltered = len(data)
    return {
              'recordsTotal': recordsTotal,
              'recordsFiltered': recordsFiltered,
              'data': data
            }

  if searches is not None:
    recordsFiltered = n_rows_filtered(cursor, clause(searches))

  if limit is None:
    limit = recordsTotal

  logger.info("Executing query with limit and offset")
  query = f"{query} LIMIT {limit} OFFSET {offset}"
  data = execute(cursor, query)
  conn.close()

  result = {
              'recordsTotal': recordsTotal,
              'recordsFiltered': recordsFiltered,
              'data': data
            }

  return result


def _dbinfo(sqldb=None, table_name=None, json_head=None, json_body=None):

  dbinfo = {}

  if sqldb is None:
    if table_name is None:
      table_name = os.path.basename(json_body).split(".")[0]
      logger.info(f"No table name given; using '{table_name}'")
    dbinfo["table_name"] = table_name
    dbinfo["jsondb"] = {"body": json_body, "head": json_head}
    dbinfo["column_names"] = _column_names(json_head=json_head, json_body=json_body)

    return dbinfo

  if table_name is None:
    table_names = _table_names(sqldb)
    table_name = ".".join(os.path.basename(sqldb).split(".")[0:-1])
    if table_name in table_names:
      logger.info(f"No table name given; using table name based on sqldb file name: '{table_name}'")
    else:
      logger.info(f"No table name given; using first table returned from list of table names: '{table_name}'")
      if len(table_names) == 0:
        logger.error(f"Error: No tables found in database file {sqldb}. Exiting.")
        return None
      table_name = table_names[0]

    table_metadata = f'{table_name}.metadata'
    if f'{table_metadata}' in table_names:
      conn = sqlite3.connect(sqldb)
      cursor = conn.cursor()
      query = f"SELECT * FROM `{table_metadata}`"
      cursor.execute(query)
      row = cursor.fetchone()
      if row is not None:
        logger.debug(f"Table metadata: {row[1]}")
      conn.close()
      dbinfo["tableMetadata"] = json.loads(row[1])

  conn = sqlite3.connect(sqldb)
  cursor = conn.cursor()
  query = f"SELECT COUNT(*) FROM `{table_name}`"
  cursor.execute(query)
  n_rows = cursor.fetchone()[0]
  dbinfo["n_rows"] = n_rows
  conn.close()

  dbinfo["sqldb"] = sqldb
  dbinfo["table_name"] = table_name
  dbinfo["column_names"] = _column_names(sqldb=sqldb, table_name=table_name)

  return dbinfo


def _data_transform(data, column_names, verbose):

  if not verbose:
    return data
  data_verbose = []
  logger.debug("Transforming data to verbose format. Column names: {column_names}")
  for row in data:
    data_verbose.append({column_names[i]: row[i] for i in range(len(column_names))})
  return data_verbose


def _column_names(sqldb=None, table_name=None, json_head=None, json_body=None):

  if sqldb is not None:
    connection = sqlite3.connect(sqldb)
    cursor = connection.cursor()
    query_ = f"SELECT * from '{table_name}';"
    try:
      logger.info(f"Executing {query_}")
      cursor.execute(query_)
      connection.close()
      column_names = [description[0] for description in cursor.description]
      logger.info(f"Found {len(column_names)} columns in {sqldb}")
      return column_names
    except Exception as e:
      print(f"Error executing query for column names using '{query_}' on {sqldb}")
      raise e

  if json_head is None:
    return list(range(0, len(json_body[0])))

  with open(json_head) as f:
    try:
      header = json.load(f)
    except Exception:
      header = list(range(0, len(json_body[0])))
    return header


def _table_names(sqldb):
  connection = sqlite3.connect(sqldb)

  query_ = "SELECT name FROM sqlite_master WHERE type='table';"
  try:
    cursor = connection.cursor()
    logger.info(f"Executing {query_}")
    cursor.execute(query_)
    table_names = []
    for row in cursor.fetchall():
      table_names.append(row[0])
  except Exception as e:
    logger.error(f"Error executing query for table names using '{query_}' on {sqldb}")
    raise e
  logger.info(f"Tables in {sqldb}: {table_names}")

  connection.close()

  return table_names
