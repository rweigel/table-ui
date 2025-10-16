import os
import copy
import json
import time
import urllib
import logging

import sqlite3
import uvicorn
import fastapi

logger = logging.getLogger(__name__)

ROOT_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
def serve(config=os.path.join(ROOT_DIR, "conf", "default.json"),
          host="0.0.0.0",
          port=5001,
          debug=False):

  if debug:
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.INFO)

  runconfig = {
                "host": host,
                "port": port,
                "server_header": False
              }

  app = fastapi.FastAPI()

  if isinstance(config, str):
    with open(config) as f:
      logger.info(f"Reading: {config}")
      try:
        configs = json.load(f)
      except Exception as e:
        emsg = f"Error executing json.load('{config}')"
        logger.error(f"{emsg}: {e}. Exiting.")
        exit(1)
  else:
    configs = config

  if isinstance(configs, dict):
    configs = [configs]

  paths = []
  dbconfigs = []
  for config in configs:
    logger.info("")
    logger.info(f"Adding database with config: {config}")
    _dbinfo(config, update=False)
    dbconfigs.append(config)
    # Create a list of paths served by this server. Will be added to
    # return of /config in dataTablesAdditions['relatedTables'].
    paths.append({"path": config['path'], "name": config['table_name']})

  for dbconfig in dbconfigs:
    if len(dbconfigs) > 1:
      dbconfig['paths'] = paths
    logger.info(f"Initalizing API for with path = '{dbconfig['path']}'")
    _api_init(app, dbconfig)

  logger.info("Starting server")
  uvicorn.run(app, **runconfig)


def _api_init(app, dbconfig):
  # Must import StaticFiles from fastapi.staticfiles
  # fastapi.staticfiles is not in dir(fastapi) (it is added dynamically)
  from fastapi.staticfiles import StaticFiles

  def cors_headers(response: fastapi.Response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, HEAD, OPTIONS"
    return response

  def parse_int(parameter, parameters, min=0, default=None):
    if parameter in parameters:
      val = parameters[parameter]
      try:
        val = int(val)
      except Exception:
        emsg = f"Error: {parameter} must be an integer"
        return fastapi.responses.JSONResponse(content={"error": emsg}, status_code=400)
      if not val >= min:
        emsg = f"Error: {parameter} >= {min} required"
    else:
      val = default

    parameters[parameter] = val

  path = ""
  if "path" in dbconfig and dbconfig['path']:
    path = "/" + dbconfig['path']

  for dir in ['js', 'css', 'demo', 'misc']:
    directory = os.path.join(ROOT_DIR, dir)
    app.mount(f"{path}/{dir}/", StaticFiles(directory=directory))

  logger.info(f"Initalizing endpoint {path}/")
  @app.route(f"{path}/", methods=["GET", "HEAD"])
  def indexhtml(request: fastapi.Request):
    # Silently ignores any query parameters
    fname = os.path.join(ROOT_DIR, 'index.html')
    logger.info("Reading: " + fname)
    with open(fname) as f:
      indexhtml_ = f.read()
    return fastapi.responses.HTMLResponse(indexhtml_)

  if "jsondb" in dbconfig:
    endpoint = f"{path}/jsondb"
    logger.info(f"Initalizing endpoint '{endpoint}'")
    @app.route(endpoint, methods=["GET", "HEAD"])
    def jsondb(request: fastapi.Request):
      # Silently ignores any query parameters other than _verbose
      query_params = dict(request.query_params)

      err = _dbinfo(dbconfig, update=True)
      if err is not None:
        return fastapi.responses.JSONResponse(content={"error": err}, status_code=500)

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
    endpoint = f"{path}/sqldb"
    logger.info(f"Initalizing endpoint '{endpoint}'")
    @app.route(f"{path}/sqldb", methods=["GET", "HEAD"])
    def sqldb(request: fastapi.Request):
      # Silently ignores any query parameters

      err = _dbinfo(dbconfig, update=True)
      if err is not None:
        return fastapi.responses.JSONResponse(content={"error": err}, status_code=500)

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

  endpoint = f"{path}/config"
  logger.info(f"Initalizing endpoint '{endpoint}'")
  @app.route(f"{path}/config", methods=["GET", "HEAD"])
  def config(request: fastapi.Request):
    # Silently ignores any query parameters

    err = _dbinfo(dbconfig, update=True)
    if err is not None:
      content = {"error": err}
      return fastapi.responses.JSONResponse(content=content, status_code=500)

    config = {
      "dataTables": dbconfig['config'],
      "dataTablesAdditions": dbconfig['dataTablesAdditions']
    }

    return fastapi.responses.JSONResponse(content=config)

  endpoint = f"{path}/render.js"
  logger.info(f"Initalizing endpoint '{endpoint}'")
  @app.route(f"{path}/render.js", methods=["GET", "HEAD"])
  def render(request: fastapi.Request):
    # Silently ignores any query parameters
    err = _dbinfo(dbconfig, update=True)
    if err is not None:
      return fastapi.responses.JSONResponse(content={"error": err}, status_code=500)

    renderFunctions = dbconfig['dataTablesAdditions']['renderFunctions']
    return fastapi.responses.FileResponse(renderFunctions, media_type='application/javascript')

  endpoint = f"{path}/data/"
  logger.info(f"Initalizing endpoint '{endpoint}'")
  @app.route(f"{path}/data/", methods=["POST", "GET", "HEAD"])
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

    err = _dbinfo(dbconfig, update=True)
    if err is not None:
      return fastapi.responses.JSONResponse(content={"error": err}, status_code=500)

    if "jsondb" in dbconfig:
      # No server-side processing. Serve entire JSON and return.
      for key in query_params.keys():
        if key not in ["_", "_verbose"]:
          emsg = f"Error: Unknown query parameter: {key}. Only '_' and '_verbose' allowed when using jsondb (=> serverSide=false)."
          return fastapi.responses.JSONResponse(content={"error": emsg}, status_code=400)

      data = dbconfig['jsondb']['data']
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

    parse_int("_start", query_params, min=0, default=0)
    parse_int("_length", query_params, min=1, default=None)

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

    try:
      result = _sql_query(dbconfig, query_params)
    except Exception as e:
      emsg = f"Error querying database: {e}"
      return fastapi.responses.JSONResponse(content={"error": emsg}, status_code=500)

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


def _dbinfo(dbconfig, update=True):

  if not update:
    if 'sqldb' not in dbconfig and 'jsondb' not in dbconfig:
      logger.error("Must specify at least sqldb or jsondb. Exiting.")
      exit(1)

    if 'sqldb' in dbconfig and 'jsondb' in dbconfig:
      logger.error("Both sqldb and jsondb were given. Choose one. Exiting.")
      exit(1)

  # Add 'table_meta' to dbconfig (default table metadata)
  emsg = _table_meta(dbconfig, update=update)
  if emsg is not None:
    return emsg

  # Add (or updates) and checks 'config' in dbconfig
  emsg = _dtconfig(dbconfig, update=update)
  if emsg is not None:
    return emsg

  if 'jsondb' in dbconfig:

    if not os.path.exists(dbconfig['jsondb']['body']):
      emsg = f"File not found: {dbconfig['jsondb']['body']}"
      return _error(emsg, "", update)

    try:
      with open(dbconfig['jsondb']['body']) as f:
        logger.info(f"Reading: {dbconfig['jsondb']['body']}")
        dbconfig['jsondb']['data'] = json.load(f)
    except Exception as e:
      emsg = f"Error reading jsondb body file {dbconfig['jsondb']['body']}"
      return _error(emsg, e, update)

    if dbconfig.get('table_name', None) is None:
      dbconfig['table_name'] = os.path.basename(dbconfig['jsondb']['body']).split(".")[0]
      wmsg = f"No table name given; using '{dbconfig['table_name']}', "
      wmsg += "which is based on file name of json_body"
      logger.warning(wmsg)

    dbconfig['path'] = dbconfig.get('path', dbconfig['table_name'])

    # Adds 'column_names' to dbconfig and 'columns' to dbconfig['config']
    emsg = _column_names(dbconfig, update=update)
    if emsg is not None:
      return emsg

    # Adds or updates 'dataTablesAdditions' in dbconfig
    emsg = _dataTablesAdditions(dbconfig, update=update)
    if emsg is not None:
      return emsg

    return None


  # Adds 'sqldb_tables' to dbconfig
  emsg = _sql_table_names(dbconfig, update=update)
  if emsg is not None:
    return emsg

  if dbconfig.get('table_name', None) is None:
    table_name = ".".join(os.path.basename(dbconfig['sqldb']).split(".")[0:-1])
    if table_name in dbconfig['sqldb_tables']:
      logger.info(f"No table_name given; Found table with name based on sqldb file name: '{table_name}'")
      dbconfig['table_name'] = table_name
    else:
      emsg = f"No table_name given and could not find table named '{table_name}' in {dbconfig['sqldb']}"
      return _error(emsg, "", update)
  else:
    if dbconfig['table_name'] not in dbconfig['sqldb_tables']:
      emsg = f"Could not find table named '{dbconfig['table_name']}' in {dbconfig['sqldb']}. Tables found: {dbconfig['sqldb_tables']}"
      return _error(emsg, "", update)

  dbconfig['path'] = dbconfig.get('path', dbconfig['table_name'])

  # Adds 'column_names' to dbconfig and 'columns' to dbconfig['config']
  emsg = _column_names(dbconfig, update=update)
  if emsg is not None:
    return emsg

  # Adds or updates 'dataTablesAdditions' in dbconfig
  emsg = _dataTablesAdditions(dbconfig, update=update)
  if emsg is not None:
    return emsg

  # Adds 'n_rows' to dbconfig
  emsg = _sql_n_rows(dbconfig, update=update)
  if emsg is not None:
    return emsg

  # Adds or updates 'dataTablesAdditions' in dbconfig
  emsg = _sql_table_meta(dbconfig, update=update)
  if emsg is not None:
    return emsg

  return None


def _dataTablesAdditions(dbconfig, update=False):

  if 'dataTablesAdditions' in dbconfig:
    dataTablesAdditions_file = dbconfig.get('dataTablesAdditions_file', None)
    if isinstance(dbconfig['dataTablesAdditions'], str) or dataTablesAdditions_file is not None:
      if isinstance(dbconfig['dataTablesAdditions'], str):
        dataTablesAdditions_file = dbconfig['dataTablesAdditions']
        dbconfig['dataTablesAdditions_file'] = dataTablesAdditions_file
      with open(dataTablesAdditions_file) as f:
        logger.info("Reading: " + dataTablesAdditions_file)
        try:
          dbconfig['dataTablesAdditions'] = json.load(f)
        except Exception as e:
          emsg = f"Error executing json.load('{dataTablesAdditions_file}')"
          return _error(emsg, e, update)

  dataTablesAdditions = dbconfig.get("dataTablesAdditions", {})

  if 'renderFunctions' not in dataTablesAdditions:
    dataTablesAdditions['renderFunctions'] = os.path.join(ROOT_DIR, 'js', 'render.js')

  if not os.path.exists(dataTablesAdditions['renderFunctions']):
    emsg = f"File not found: {dataTablesAdditions['renderFunctions']}"
    return _error(emsg, "", update)

  if "paths" in dbconfig:
    dataTablesAdditions['relatedTables'] = dbconfig['paths']

  print(dbconfig)

  if "sqldb" in dbconfig:
    dbfile = dbconfig["sqldb"]
    dataTablesAdditions['sqldb'] = os.path.basename(dbfile)

  if "jsondb" in dbconfig:
    dbfile = dbconfig["jsondb"]["body"]
    dataTablesAdditions['jsondb'] = os.path.basename(dbfile)

  # Merge table_meta from dbconfig and dataTablesAdditions
  table_meta = copy.deepcopy(dbconfig.get('table_meta', None))
  if table_meta is not None:
    # Overwrite any existing keys in dbconfig['table_meta'] (defaults)
    # with those in tableMetadata
    table_meta = {**table_meta, **dataTablesAdditions.get('tableMetadata', {})}
    dataTablesAdditions["tableMetadata"] = table_meta

  if dataTablesAdditions.get('tableMetadata', None) is None:
    dataTablesAdditions['tableMetadata'] = {}

  if 'tableName' not in dataTablesAdditions['tableMetadata']:
    dataTablesAdditions['tableMetadata']['tableName'] = dbconfig['table_name']
  else:
    if dataTablesAdditions['tableMetadata']['tableName'] != dbconfig['table_name']:
      dataTablesAdditions['tableMetadata']['tableName'] = dbconfig['table_name']
      if not update:
        wmsg = "tableMetadata.tableName does not match config.table_name. "
        wmsg += f"Overriding to config.table_name = {dbconfig['table_name']}"
        logger.warning(wmsg)

  if dataTablesAdditions['tableMetadata'].get('creationDate', None) is None:
    try:
      import datetime
      mtime = os.path.getmtime(dbfile)
      creationDate = datetime.datetime.fromtimestamp(mtime).isoformat()
      dataTablesAdditions['tableMetadata']['creationDate'] = creationDate[0:-7] + "Z"
    except Exception as e:
      logger.warning(f"Could not get file modification time for {dbfile}: {e}")

  dbconfig['dataTablesAdditions'] = dataTablesAdditions


def _dtconfig(dbconfig, update=False):

  if 'config' not in dbconfig:
    default = os.path.join(ROOT_DIR, 'conf', 'default.json')
    dbconfig['config'] = default

  config_file = dbconfig.get('config_file', None)
  if isinstance(dbconfig['config'], str) or config_file is not None:
    if isinstance(dbconfig['config'], str):
      config_file = dbconfig['config']
      dbconfig['config_file'] = config_file
    with open(config_file) as f:
      logger.info("Reading: " + config_file)
      try:
        dbconfig['config'] = json.load(f)
      except Exception as e:
        emsg = f"Error executing json.load('{config_file}')"
        return _error(emsg, e, update)

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


def _error(emsg, err, update):
  if update:
    logger.error(f"{emsg}.")
    return emsg
  logger.error(f"{emsg}: {err}. Exiting.")
  exit(1)


def _data_transform(data, column_names, verbose):

  if not verbose:
    return data
  data_verbose = []
  logger.debug(f"Transforming data to verbose format. Column names: {column_names}")
  for row in data:
    data_verbose.append({column_names[i]: row[i] for i in range(len(column_names))})
  return data_verbose


def _column_names(dbconfig, update=False):

  def set_config_columns(column_names):
    dbconfig["config"]['columns'] = []
    for column_name in column_names:
      dbconfig["config"]['columns'].append({"name": column_name})

  if 'sqldb' in dbconfig:
    query = f"PRAGMA table_info('{dbconfig['table_name']}');"
    try:
      logger.info("Getting column names")
      data = _sql_execute(query, sqldb=dbconfig['sqldb'])
      logger.info(f"Got {len(data)} column names\n")
      dbconfig['column_names'] = [row[1] for row in data]
    except Exception as e:
      emsg = "Error getting column names"
      return _error(emsg, e, update)

    set_config_columns(dbconfig['column_names'])
    return None

  n_rows = len(dbconfig['jsondb']['data'][0])
  column_names = [str(c) for c in range(0, n_rows)]
  if dbconfig['jsondb'].get('head', None) is None:
    if not update:
      logger.warning("No json_head file given. Using indices for column names.")
    set_config_columns(column_names)
    dbconfig['column_names'] = column_names
    return None

  if not os.path.exists(dbconfig['jsondb']['head']):
    emsg = f"File not found: {dbconfig['jsondb']['head']}"
    _error(emsg, "", update)

  with open(dbconfig['jsondb']['head']) as f:
    try:
      column_names = json.load(f)
      set_config_columns(column_names)
      dbconfig['column_names'] = column_names
      return None
    except Exception as e:
      emsg = f"Error executing json.load('{dbconfig['jsondb']['head']}')"
      return _error(emsg, e, update)


def _table_meta(dbconfig, update=False):

  if dbconfig.get('table_meta', None) is None:
    if 'jsondb' in dbconfig:
      if not update:
        logger.warning("No table_meta file given.")
  else:
    if isinstance(dbconfig['table_meta'], str) and os.path.exists(dbconfig['table_meta']):
      with open(dbconfig['table_meta']) as f:
        try:
          dbconfig['table_meta'] = json.load(f)
        except Exception as e:
          emsg = f"Error executing json.load('{dbconfig['table_meta']}')"
          return _error(emsg, e, update)

  return None


def _sql_execute(query, cursor=None, sqldb=None, params=None):
  if sqldb is not None:
    connection = sqlite3.connect(sqldb)
    cursor = connection.cursor()
  start = time.time()
  logger.info("  Executing")
  logger.info(f"  {query}")
  logger.info("  and fetching all results from")
  logger.info(f"  {sqldb if sqldb is not None else 'existing connection'}")
  if params:
      result = cursor.execute(query, params)
  else:
      result = cursor.execute(query)
  data = result.fetchall()
  if sqldb is not None:
    connection.close()
  dt = "{:.4f} [s]".format(time.time() - start)
  n_rows = len(data)
  n_cols = len(data[0]) if n_rows > 0 else 0
  logger.info(f"  {dt} to execute query and fetch {n_rows}x{n_cols} table.")
  return data


def _sql_query(dbinfo, query_params):

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
        return "", []
    keys = list(searches.keys())
    where = []
    params = []
    escape = "\\"
    for key in keys:
        val = searches[key]
        if val == "''" or val == '""':
            where.append(f"`{key}` = ?")
            params.append('')
        elif val.startswith("'") and val.endswith("'"):
            where.append(f"`{key}` = ?")
            params.append(val.strip("'"))
        elif val.startswith('%') and not val.endswith('%'):
            where.append(f"`{key}` LIKE ? ESCAPE '{escape}'")
            params.append(val)
        elif not val.startswith('%') and val.endswith('%'):
            where.append(f"`{key}` LIKE ? ESCAPE '{escape}'")
            params.append(val)
        else:
            where.append(f"`{key}` LIKE ? ESCAPE '{escape}'")
            params.append(f"%{val}%")
    if len(where) == 0:
        return "", []
    return "WHERE " + " AND ".join(where), params

  offset = query_params['_start']
  limit = query_params['_length']
  orders = query_params['_orders']
  searches = query_params['searches']
  _return = query_params['_return']
  uniques = query_params['_uniques']

  recordsTotal = dbinfo['n_rows']
  recordsFiltered = recordsTotal

  clause_str, clause_params = clause(searches)

  if uniques:
    uniques = {}
    columns = _return
    if _return is None:
      columns = dbinfo['column_names']
    for col in columns:
      logger.info(f"Getting unique values for column '{col}'")
      query = f"SELECT `{col}`, COUNT(*) as count FROM `{dbinfo['table_name']}` {clause_str} GROUP BY `{col}`"
      rows = _sql_execute(query, sqldb=dbinfo['sqldb'], params=clause_params)
      logger.info(f"Got {len(rows)} unique values\n")
      # Each value is a list of (value, count) tuples
      uniques[col] = rows.copy()
    return {"data": uniques}

  if _return is None:
    columns_str = "*"
  else:
    columns_str = ", ".join([f"`{col}`" for col in _return])

  query = f"SELECT {columns_str} FROM `{dbinfo['table_name']}` {clause_str} {orderby(orders)}"
  if offset == 0 and limit is None:
    logger.info("No _start or _length given. Extracting all records.")
    data = _sql_execute(query, sqldb=dbinfo['sqldb'], params=clause_params)
    if searches is not None:
      recordsFiltered = len(data)
    return {
              'recordsTotal': recordsTotal,
              'recordsFiltered': recordsFiltered,
              'data': data
            }

  if searches is not None:
    query_count = f"SELECT COUNT(*) FROM `{dbinfo['table_name']}` {clause_str}"
    logger.info("Getting number of filtered records")
    recordsFiltered = _sql_execute(query_count, sqldb=dbinfo['sqldb'], params=clause_params)[0][0]
    logger.info(f"Got number of filtered records = {recordsFiltered}\n")

  if limit is None:
    limit = recordsTotal

  warning = None
  if offset >= recordsFiltered:
    warning = f"_start={offset} is larger than the number of filtered records ({recordsFiltered})."
    warning += f" Setting _start to {max(0, recordsFiltered - limit)}"
    offset = max(0, recordsFiltered - limit)

  if offset + limit > recordsFiltered:
    warning = f"_start + _length = {offset + limit} is larger than the number of filtered records ({recordsFiltered})."
    warning += f" Setting _length to {recordsFiltered - offset}"
    limit = recordsFiltered - offset

  query = f"{query} LIMIT {limit} OFFSET {offset}"
  logger.info(f"Getting records with offset={offset} and limit={limit}")
  data = _sql_execute(query, sqldb=dbinfo['sqldb'], params=clause_params)
  logger.info(f"Got {len(data)} records\n")

  result = {
              'recordsTotal': recordsTotal,
              'recordsFiltered': recordsFiltered,
              'data': data
            }
  if warning is not None:
    result['warning'] = warning

  return result


def _sql_table_meta(dbconfig, update=False):

  table_metadata = f"{dbconfig['table_name']}.metadata"
  if f'{table_metadata}' in dbconfig['sqldb_tables']:
    if 'table_meta' in dbconfig and dbconfig['table_meta'] is not None:
      if update:
        logger.warning(f"Warning: table_meta already given and will be over-written by {table_metadata} table in {dbconfig['sqldb']}")
    try:
      query = f"SELECT * FROM `{table_metadata}`"
      logger.info("Getting table metadata")
      row = _sql_execute(query, sqldb=dbconfig['sqldb'])[0]
      if row is not None:
        dbconfig['table_meta'] = json.loads(row[1])
        keys = list(dbconfig['table_meta'].keys())
        logger.info(f"Got table metadata with keys: {keys}\n")
      else:
        logger.info("No table metadata\n")
        return None
    except Exception as e:
      emsg = "Error getting table metadata"
      return _error(emsg, e, update)


def _sql_n_rows(dbconfig, update=False):
  try:
    query = f"SELECT COUNT(*) FROM `{dbconfig['table_name']}`"
    logger.info("Getting number of rows")
    dbconfig["n_rows"] = _sql_execute(query, sqldb=dbconfig['sqldb'])[0][0]
    logger.info(f"Got number of rows = {dbconfig['n_rows']}\n")
  except Exception as e:
    emsg = f"Error getting number of rows in {dbconfig['table_name']}"
    return _error(emsg, e, update)

  return None


def _sql_table_names(dbconfig, update=False):

  query = "SELECT name FROM sqlite_master WHERE type='table';"
  try:
    table_names = []
    logger.info("Getting table names")
    data = _sql_execute(query, sqldb=dbconfig['sqldb'])
    for row in data:
      table_names.append(row[0])
    logger.info("Got tables names")
    logger.info(f"  {table_names}\n")
  except Exception as e:
    emsg = "Error getting table names"
    return _error(emsg, e, update)

  dbconfig['sqldb_tables'] = table_names

  return None
