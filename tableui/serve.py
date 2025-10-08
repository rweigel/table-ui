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

# Default root_dir is the parent directory of this file's directory
root_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
def serve(table_name=None,
          table_meta=None,
          sqldb=None,
          json_head=None,
          json_body=None,
          js_render="js/render.js",
          config="config.json",
          root_dir=root_dir,
          host="0.0.0.0",
          port=5001,
          debug=False):

  if debug:
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.INFO)

  dbconfig = {
    "root_dir": root_dir,
    "table_name": table_name,
    "table_meta": table_meta,
    "sqldb": sqldb,
    "js_render": js_render,
    "config": config
  }
  if json_body is not None:
    del dbconfig['sqldb']
    dbconfig['jsondb'] = {
      "head": json_head,
      "body": json_body
    }

  _dbinfo(dbconfig, update=False)

  runconfig = {
                "host": host,
                "port": port,
                "server_header": False
              }

  app = fastapi.FastAPI()

  logger.info("Initalizing API")
  _api_init(app, dbconfig)

  logger.info("Starting server")
  uvicorn.run(app, **runconfig)


def _api_init(app, dbconfig):
  # Must import StaticFiles from fastapi.staticfiles
  # fastapi.staticfiles is not in dir(fastapi) (it is added dynamically)
  from fastapi.staticfiles import StaticFiles

  _dbinfo(dbconfig)

  def cors_headers(response: fastapi.Response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, HEAD, OPTIONS"
    return response

  for dir in ['js', 'css', 'img', 'demo', 'misc']:
    directory = os.path.join(dbconfig['root_dir'], dir)
    app.mount(f"/{dir}/", StaticFiles(directory=directory))

  @app.route("/", methods=["GET", "HEAD"])
  def indexhtml(request: fastapi.Request):
    # Silently ignores any query parameters
    fname = os.path.join(dbconfig['root_dir'], 'index.html')
    logger.info("Reading: " + fname)
    with open(fname) as f:
      indexhtml_ = f.read()
    return fastapi.responses.HTMLResponse(indexhtml_)

  if "jsondb" in dbconfig:
    @app.route("/jsondb", methods=["GET", "HEAD"])
    def jsondb(request: fastapi.Request):
      # Silently ignores any query parameters other than _verbose
      query_params = dict(request.query_params)

      # Update
      _dbinfo(dbconfig)

      with open(dbconfig['jsondb']['body']) as f:
        # TODO: Stream
        logger.info("Reading and sending: " + dbconfig['jsondb']['body'])
        data = json.load(f)
        # TODO: Use _verbose validation in data()
        data = _data_transform(data, dbconfig['column_names'], query_params.get("_verbose", None) == "true")
      data = {
        "columns": dbconfig['column_names'],
        "data": data
      }
      return fastapi.responses.JSONResponse(data)

  if "sqldb" in dbconfig and dbconfig["sqldb"] is not None:
    @app.route("/sqldb", methods=["GET", "HEAD"])
    def sqldb(request: fastapi.Request):
      # Silently ignores any query parameters

      # Update
      _dbinfo(dbconfig)

      filename = os.path.basename(dbconfig['sqldb'])
      if filename.endswith('.sqlite'):
        filename = filename[0:-7] + '.sqlite3'
      if filename.endswith('.sql'):
        filename = filename[0:-4] + '.sqlite3'
        kwargs = {
                    'media_type': 'application/x-sqlite3',
                    'filename': filename
                  }
      return fastapi.responses.FileResponse(dbconfig['sqldb'], **kwargs)

  @app.route("/config", methods=["GET", "HEAD"])
  def config(request: fastapi.Request):
    # Silently ignores any query parameters

    # Update
    _dbinfo(dbconfig)

    config = dbconfig['config']
    config['tableUI'] = {"tableMetadata": dbconfig.get('table_meta', {})}
    config['tableUI']['tableMetadata']['name'] = dbconfig["table_name"]
    if "sqldb" in dbconfig:
      config['tableUI']['sqldb'] = dbconfig["sqldb"]
      dbfile = dbconfig["sqldb"]
    if "jsondb" in dbconfig:
      config['tableUI']['jsondb'] = dbconfig["jsondb"]
      dbfile = dbconfig["jsondb"]["body"]

    if config['tableUI']['tableMetadata'].get('creationDate', None) is None:
      try:
        import datetime
        mtime = os.path.getmtime(dbfile)
        creationDate = datetime.datetime.fromtimestamp(mtime).isoformat()
        config['tableUI']['tableMetadata']['creationDate'] = creationDate[0:-7] + "Z"
      except Exception as e:
        logger.warning(f"Could not get file modification time for {dbfile}: {e}")

    return fastapi.responses.JSONResponse(content=config)

  @app.route("/render.js", methods=["GET", "HEAD"])
  def render(request: fastapi.Request):
    # Silently ignores any query parameters
    return fastapi.responses.FileResponse(dbconfig['js_render'], media_type='application/javascript')

  @app.route("/header", methods=["GET", "HEAD"])
  def header(request: fastapi.Request):
    # Silently ignores any query parameters

    # Update
    _dbinfo(dbconfig)

    return fastapi.responses.JSONResponse(content=dbconfig['column_names'])

  @app.route("/data/", methods=["POST", "GET", "HEAD"])
  def data(request: fastapi.Request):

    logger.info(f"Received data request with query params: {request.query_params}")
    query_params = dict(request.query_params)

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

    _dbinfo(dbconfig)

    if "jsondb" in dbconfig:
      # No server-side processing. Serve entire JSON and return.
      for key in query_params.keys():
        if key not in ["_", "_verbose"]:
          emsg = f"Error: Unknown query parameter: {key}. Only '_' and '_verbose' allowed when using jsondb (=> serverSide=false)."
          return fastapi.responses.JSONResponse(content={"error": emsg}, status_code=400)

      print(dbconfig)
      try:
        with open(dbconfig['jsondb']['body']) as f:
          # TODO: Stream
          logger.info("Reading and sending: " + dbconfig['jsondb']['body'])
          data = json.load(f)
      except Exception as e:
        emsg = f"Error reading jsondb body file {dbconfig['jsondb']['body']}: {e}"
        return fastapi.responses.JSONResponse(content={"error": emsg}, status_code=400)

      data = _data_transform(data, dbconfig['column_names'], query_params["_verbose"])
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

    for key in query_params.keys():
      if key not in keys_allowed and key not in dbconfig['column_names']:
        logger.error(f"Error: Unknown query parameter: {key}.")
        emsg = f"Error: Unknown query parameter with first five character of {key[0:5]}. Allowed: {keys_allowed} and "
        emsg += f"column names: {dbconfig['column_names']}"
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
        if col not in dbconfig['column_names']:
          emsg = f"Error: _orders column '{col}' not found in column names: "
          emsg += "{dbconfig['column_names']}"
          return fastapi.responses.JSONResponse(content={"error": emsg}, status_code=400)
      query_params["_orders"] = orders
    else:
      query_params["_orders"] = None

    searches = {}
    if query_params is not None:
      logger.info(f"Query params: {query_params}")
      for key, _ in query_params.items():
        if key in dbconfig['column_names']:
          kwargs = {'encoding': 'utf-8', 'errors': 'replace'}
          searches[key] = urllib.parse.unquote(query_params[key], **kwargs)
      query_params['searches'] = searches
    else:
      query_params['searches'] = None

    return_cols = dbconfig['column_names']
    if "_return" in query_params:
      query_params["_return"] = query_params["_return"].split(",")
      for col in query_params["_return"]:
        if col not in dbconfig['column_names']:
          emsg = f"Error: _return column '{col}' not found in column names: "
          emsg += "{dbconfig['column_names']}"
          return fastapi.responses.JSONResponse(content={"error": emsg}, status_code=400)
      return_cols = query_params["_return"]
    else:
      query_params["_return"] = None

    result = _dbquery(dbconfig, query_params)

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


def _dbinfo(dbconfig, update=True):

  if not update:
    for key in ['root_dir', 'table_meta', 'sqldb', 'js_render', 'config']:
      if key in dbconfig and isinstance(dbconfig[key], str):
        if not os.path.isabs(dbconfig[key]):
          dbconfig[key] = os.path.join(dbconfig['root_dir'], dbconfig[key])

    if 'jsondb' in dbconfig:
      for key in ['head', 'body']:
        if dbconfig['jsondb'][key] is not None and not os.path.isabs(dbconfig['jsondb'][key]):
          dbconfig['jsondb'][key] = os.path.join(dbconfig['root_dir'], dbconfig['jsondb'][key])

    if 'sqldb' not in dbconfig and 'jsondb' not in dbconfig:
      logger.error("Must specify at least --sqldb or --json_body. Exiting.")
      exit(1)

    if 'sqldb' in dbconfig and 'jsondb' in dbconfig:
      logger.error("Both sqldb and jsondb were given. Choose one. Exiting.")
      exit(1)

  if dbconfig['table_meta'] is None:
    if 'jsondb' in dbconfig:
      if not update:
        logger.warning("No table_meta file given.")
  else:
    if isinstance(dbconfig['table_meta'], str) and os.path.exists(dbconfig['table_meta']):
      with open(dbconfig['table_meta']) as f:
        try:
          dbconfig['table_meta'] = json.load(f)
        except Exception as e:
          emsg = f"Error reading table_meta file {dbconfig['table_meta']}"
          if update:
            return emsg
          logger.error(f"{emsg}: {e}. Exiting.")
          exit(1)
    else:
      emsg = f"File not found: {dbconfig['table_meta']}"
      if update:
        return emsg
      else:
        logger.error(f"{emsg}. Exiting.")
        exit(1)

  _dtconfig(dbconfig, update=update)

  if 'jsondb' in dbconfig:

    if not os.path.exists(dbconfig['jsondb']['body']):
      emsg = f"File not found: {dbconfig['jsondb']['body']}"
      if not update:
        logger.error(f"{emsg}. Exiting.")
        exit(1)
      else:
        return emsg

    if dbconfig['table_name'] is None:
      dbconfig['table_name'] = os.path.basename(dbconfig['jsondb']['body']).split(".")[0]
      logger.info(f"No table name given; using '{dbconfig['table_name']}', which is based on file name of json_body")

    if dbconfig['jsondb']['head'] is None:
      if not update:
        logger.warning("No json_head file given. Using indices for column names.")
    else:
      if not os.path.exists(dbconfig['jsondb']['head']):
        emsg = f"File not found: {dbconfig['jsondb']['head']}"
        if not update:
          logger.error(f"{emsg}. Exiting.")
          exit(1)
        else:
          return emsg

    dbconfig["column_names"] = _column_names(json_head=dbconfig['jsondb']['head'], json_body=dbconfig['jsondb']['body'])

    return dbconfig

  table_names = _table_names(dbconfig['sqldb'])
  if dbconfig['table_name'] is None:
    table_name = ".".join(os.path.basename(dbconfig['sqldb']).split(".")[0:-1])
    if table_name in table_names:
      logger.info(f"No table_name given; using table name based on sqldb file name: '{table_name}'")
      dbconfig['table_name'] = table_name
    else:
      emsg = f"No table_name given and could not find table named '{table_name}' in {dbconfig['sqldb']}"
      if not update:
        logger.error(f"{emsg}. Exiting.")
        exit(1)
      else:
        return emsg
  else:
    if dbconfig['table_name'] not in table_names:
      emsg = f"Could not find table named '{dbconfig['table_name']}' in {dbconfig['sqldb']}. Tables found: {table_names}"
      if not update:
        logger.error(f"{emsg}. Exiting.")
        exit(1)
      else:
        return emsg

    table_metadata = f"{dbconfig['table_name']}.metadata"
    if f'{table_metadata}' in table_names and dbconfig['table_meta'] is None:
      conn = sqlite3.connect(dbconfig['sqldb'])
      cursor = conn.cursor()
      query = f"SELECT * FROM `{table_metadata}`"
      cursor.execute(query)
      row = cursor.fetchone()
      if row is not None:
        logger.debug(f"Table metadata: {row[1]}")
      conn.close()
      dbconfig["table_meta"] = json.loads(row[1])
    if dbconfig['table_meta'] is None:
      if not update:
        logger.warning(f"No table_meta file given and no table metadata '{table_metadata}' found in database.")

  try:
    conn = sqlite3.connect(dbconfig['sqldb'])
    cursor = conn.cursor()
    query = f"SELECT COUNT(*) FROM `{dbconfig['table_name']}`"
    cursor.execute(query)
    n_rows = cursor.fetchone()[0]
    dbconfig["n_rows"] = n_rows
    conn.close()
  except Exception as e:
    emsg = f"Error executing query for number of rows using '{query}' on {dbconfig['sqldb']}"
    if not update:
      logger.error(f"{emsg}: {e}. Exiting.")
      exit(1)
    else:
      return emsg

  dbconfig["column_names"] = _column_names(sqldb=dbconfig['sqldb'], table_name=dbconfig['table_name'])

def _dtconfig(dbconfig, update=False):

  if not isinstance(dbconfig['config'], dict):
    with open(dbconfig['config']) as f:
      logger.info("Reading: " + dbconfig['config'])
      try:
        dbconfig['config'] = json.load(f)
      except Exception as e:
        emsg = f"Error reading config file {dbconfig['config']}"
        if not update:
          logger.error(f"{emsg}: {e}. Exiting.")
          exit(1)
        else:
          return emsg

  serverSide = dbconfig['config'].get('serverSide', None)

  if serverSide is None or serverSide not in [True, False]:
    if 'sqldb' in dbconfig:
      dbconfig['config']['serverSide'] = True
    if 'jsondb' in dbconfig:
      dbconfig['config']['serverSide'] = False
  else:
    if 'sqldb' in dbconfig and not serverSide:
      if update:
        logger.warning("Warning: Config file specifies serverSide=false but input is sqldb. Overriding to serverSide=true")
      dbconfig['config']['serverSide'] = True
    if 'jsondb' in dbconfig and serverSide:
      if update:
        logger.warning("Warning: Config file specifies serverSide=true but jsondb was given. Overriding to serverSide=false")
      dbconfig['config']['serverSide'] = False

def _data_transform(data, column_names, verbose):

  if not verbose:
    return data
  data_verbose = []
  logger.debug(f"Transforming data to verbose format. Column names: {column_names}")
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
    except Exception as e:
      print(f"Error executing query for column names using '{query_}' on {sqldb}")
      raise e
    logger.info(f"Found {len(column_names)} columns in {sqldb}")
    return column_names

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
