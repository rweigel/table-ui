import os
import json
import logging
import time
import urllib

import sqlite3
import uvicorn
import fastapi

logger = logging.getLogger(__name__)

# Default root_dir is the parent directory of this file's directory
root_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
def serve(table_name=None,
          sqldb=None,
          json_head=None,
          json_body=None,
          js_render="js/render.js",
          config="conf/default.json",
          root_dir=root_dir,
          host="0.0.0.0",
          port=5001,
          debug=False):

  if debug:
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.INFO)

  if isinstance(sqldb, list) and len(sqldb) == 1:
    sqldb = sqldb[0]

  dbconfig = {
    "root_dir": root_dir,
    "table_name": table_name,
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

  runconfig = {
                "host": host,
                "port": port,
                "server_header": False
              }

  app = fastapi.FastAPI()

  if isinstance(sqldb, (str, None.__class__)):
    # Serve single sql or json db
    _dbinfo(dbconfig, update=False)
    logger.info("Initalizing API")
    _api_init(app, dbconfig)
    logger.info("Starting server")
    uvicorn.run(app, **runconfig)
    return

  # Serve multiple sqldbs (experimental)
  root_paths = []
  dbconfigs = []
  for idx, db in enumerate(dbconfig['sqldb']):
    dbconfigs.append(dbconfig.copy())
    root_path = os.path.splitext(os.path.basename(db))[0]
    dbconfigs[-1]['root_path'] = root_path
    dbconfigs[-1]['sqldb'] = db
    # Get table name
    _dbinfo(dbconfigs[-1], update=False)
    root_paths.append({"path": root_path, "name": dbconfigs[-1]['table_name']})
    print(dbconfigs[-1]['root_path'])

  for dbconfig in dbconfigs:
    dbconfig['root_paths'] = root_paths
    logger.info(f"Initalizing API for {db} with root_path = '{dbconfig['root_path']}'")
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

  def rm_root_dir(path):
    if path is None:
      return None
    path = path.replace(dbconfig['root_dir'] + "/", "")
    path = path.replace(os.path.expanduser('~') + "/", "~/")
    return path

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

  root_path = ""
  if "root_path" in dbconfig:
    root_path = "/" + dbconfig['root_path']

  for dir in ['js', 'css', 'img', 'demo', 'misc']:
    directory = os.path.join(dbconfig['root_dir'], dir)
    app.mount(f"{root_path}/{dir}/", StaticFiles(directory=directory))

  #print(dbconfig['root_dir'])
  #print(dbconfig['root_paths'])

  logger.info(f"Initalizing {root_path}/")
  @app.route(f"{root_path}/", methods=["GET", "HEAD"])
  def indexhtml(request: fastapi.Request):
    # Silently ignores any query parameters
    fname = os.path.join(dbconfig['root_dir'], 'index.html')
    logger.info("Reading: " + fname)
    with open(fname) as f:
      indexhtml_ = f.read()
    return fastapi.responses.HTMLResponse(indexhtml_)

  if "jsondb" in dbconfig:
    @app.route(f"/{root_path}/jsondb", methods=["GET", "HEAD"])
    def jsondb(request: fastapi.Request):
      # Silently ignores any query parameters other than _verbose
      query_params = dict(request.query_params)

      err = _dbinfo(dbconfig, update=True)
      if err is not None:
        return fastapi.responses.JSONResponse(content={"error": rm_root_dir(err)}, status_code=500)

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
    @app.route(f"{root_path}/sqldb", methods=["GET", "HEAD"])
    def sqldb(request: fastapi.Request):
      # Silently ignores any query parameters

      err = _dbinfo(dbconfig, update=True)
      if err is not None:
        return fastapi.responses.JSONResponse(content={"error": rm_root_dir(err)}, status_code=500)

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

  @app.route(f"{root_path}/config", methods=["GET", "HEAD"])
  def config(request: fastapi.Request):
    # Silently ignores any query parameters

    err = _dbinfo(dbconfig, update=True)
    if err is not None:
      content = {"error": rm_root_dir(err)}
      return fastapi.responses.JSONResponse(content=content, status_code=500)

    config = dbconfig['config']
    config['tableUI'] = _tableUI(dbconfig)

    return fastapi.responses.JSONResponse(content=config)

  @app.route(f"{root_path}/render.js", methods=["GET", "HEAD"])
  def render(request: fastapi.Request):
    # Silently ignores any query parameters
    err = _dbinfo(dbconfig, update=True)
    if err is not None:
      return fastapi.responses.JSONResponse(content={"error": rm_root_dir(err)}, status_code=500)

    return fastapi.responses.FileResponse(dbconfig['js_render'], media_type='application/javascript')

  @app.route(f"{root_path}/data/", methods=["POST", "GET", "HEAD"])
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
      return fastapi.responses.JSONResponse(content={"error": rm_root_dir(err)}, status_code=500)

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
    for key in ['root_dir', 'sqldb', 'js_render', 'config']:
      if key in dbconfig and isinstance(dbconfig[key], str):
        if not os.path.isabs(dbconfig[key]):
          logger.info(f"Converting {key} = '{dbconfig[key]}' to absolute path")
          dbconfig[key] = os.path.normpath(os.path.join(dbconfig['root_dir'], dbconfig[key]))
        if not dbconfig[key].startswith(os.path.expanduser('~')):
          logger.warning(f"Path = '{dbconfig[key]}' starts with home directory")
          logger.warning("  and will not be converted to a path relative to root")
          logger.warning("  in server error messages.")
        if not dbconfig[key].startswith(dbconfig['root_dir']):
          logger.warning(f"Path = '{dbconfig[key]}' does not start with")
          logger.warning(f"  root_dir = '{dbconfig['root_dir']}'")
          logger.warning("  and will not be converted to a path relative to root")
          logger.warning("  in server error messages.")

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

  if not os.path.exists(dbconfig['js_render']):
    emsg = f"File not found: {dbconfig['js_render']}"
    if update:
      logger.error(f"{emsg}.")
      return emsg
    logger.error(f"{emsg}. Exiting.")
    exit(1)

  # Adds 'table_meta' to dbconfig
  emsg = _table_meta(dbconfig, update=update)
  if emsg is not None:
    return emsg

  emsg = _dtconfig(dbconfig, update=update)
  if emsg is not None:
    return emsg


  if 'jsondb' in dbconfig:

    if not os.path.exists(dbconfig['jsondb']['body']):
      emsg = f"File not found: {dbconfig['jsondb']['body']}"
      if update:
        logger.error(f"{emsg}.")
        return emsg
      logger.error(f"{emsg}. Exiting.")
      exit(1)

    try:
      with open(dbconfig['jsondb']['body']) as f:
        # TODO: Stream
        logger.info(f"Reading: {dbconfig['jsondb']['body']}")
        dbconfig['jsondb']['data'] = json.load(f)
    except Exception as e:
      emsg = f"Error reading jsondb body file {dbconfig['jsondb']['body']}"
      if update:
        logger.error(f"{emsg}.")
        return emsg
      logger.error(f"{emsg}: {e}. Exiting.")
      exit(1)

    if dbconfig['table_name'] is None:
      dbconfig['table_name'] = os.path.basename(dbconfig['jsondb']['body']).split(".")[0]
      wmsg = f"No table name given; using '{dbconfig['table_name']}', "
      wmsg += "which is based on file name of json_body"
      logger.warning(wmsg)

    # Adds 'column_names' to dbconfig and 'columns' to dbconfig['config']
    emsg = _column_names(dbconfig, update=update)
    if emsg is not None:
      return emsg

    return None


  # Adds 'sqldb_tables' to dbconfig
  emsg = _sql_table_names(dbconfig, update=update)
  if emsg is not None:
    return emsg

  if dbconfig['table_name'] is None:
    table_name = ".".join(os.path.basename(dbconfig['sqldb']).split(".")[0:-1])
    if table_name in dbconfig['sqldb_tables']:
      logger.info(f"No table_name given; Found table with name based on sqldb file name: '{table_name}'")
      dbconfig['table_name'] = table_name
    else:
      emsg = f"No table_name given and could not find table named '{table_name}' in {dbconfig['sqldb']}"
      if update:
        logger.error(f"{emsg}.")
        return emsg
      logger.error(f"{emsg}. Exiting.")
      exit(1)
  else:
    if dbconfig['table_name'] not in dbconfig['sqldb_tables']:
      emsg = f"Could not find table named '{dbconfig['table_name']}' in {dbconfig['sqldb']}. Tables found: {dbconfig['sqldb_tables']}"
      if update:
        logger.error(f"{emsg}.")
        return emsg
      logger.error(f"{emsg}. Exiting.")
      exit(1)

  # Adds 'column_names' to dbconfig and 'columns' to dbconfig['config']
  emsg = _column_names(dbconfig, update=update)
  if emsg is not None:
    return emsg

  # Adds 'n_rows' to dbconfig
  emsg = _sql_n_rows(dbconfig, update=update)
  if emsg is not None:
    return emsg

  # Adds or updates 'table_meta'
  emsg = _sql_table_meta(dbconfig, update=update)
  if emsg is not None:
    return emsg

  return None


def _tableUI(dbconfig):

  tableUI = {"tableMetadata": {}}
  if dbconfig.get('table_meta', None) is not None:
    tableUI = {"tableMetadata": dbconfig['table_meta']}

  tableUI['tableMetadata']['name'] = dbconfig["table_name"]
  if "root_paths" in dbconfig:
    tableUI['relatedTables'] = dbconfig['root_paths']

  if "sqldb" in dbconfig:
    dbfile = dbconfig["sqldb"]
    tableUI['sqldb'] = os.path.basename(dbfile)
  if "jsondb" in dbconfig:
    dbfile = dbconfig["jsondb"]["body"]
    tableUI['jsondb'] = os.path.basename(dbfile)
  if tableUI['tableMetadata'].get('creationDate', None) is None:
    try:
      import datetime
      mtime = os.path.getmtime(dbfile)
      creationDate = datetime.datetime.fromtimestamp(mtime).isoformat()
      tableUI['tableMetadata']['creationDate'] = creationDate[0:-7] + "Z"
    except Exception as e:
      logger.warning(f"Could not get file modification time for {dbfile}: {e}")

  return tableUI


def _dtconfig(dbconfig, update=False):

  if isinstance(dbconfig['config'], str):
    with open(dbconfig['config']) as f:
      logger.info("Reading: " + dbconfig['config'])
      try:
        config = json.load(f)
        dbconfig.update(config) # Over-writes any existing keys in dbconfig
      except Exception as e:
        emsg = f"Error executing json.load('{dbconfig['config']}')"
        if not update:
          logger.error(f"{emsg}: {e}. Exiting.")
          exit(1)
        else:
          logger.error(f"{emsg}.")
          return emsg

  if 'config' not in dbconfig['config']:
    default = os.path.join(dbconfig['root_dir'], 'conf', 'default.json')
    with open(default) as f:
      logger.info(f"Reading: {default}")
      default = json.load(f)
      dbconfig['config'] = default

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
      if update:
        logger.error(f"{emsg}.")
        return emsg
      logger.error(f"{emsg}: {e}. Exiting.")
      exit(1)

    set_config_columns(dbconfig['column_names'])
    return None

  n_rows = len(dbconfig['jsondb']['data'][0])
  column_names = [str(c) for c in range(0, n_rows)]
  if dbconfig['jsondb']['head'] is None:
    if not update:
      logger.warning("No json_head file given. Using indices for column names.")
    set_config_columns(column_names)
    dbconfig['column_names'] = column_names
    return None

  if not os.path.exists(dbconfig['jsondb']['head']):
    emsg = f"File not found: {dbconfig['jsondb']['head']}"
    if update:
      logger.error(f"{emsg}.")
      return emsg
    logger.error(f"{emsg}. Exiting.")
    exit(1)

  with open(dbconfig['jsondb']['head']) as f:
    try:
      column_names = json.load(f)
      set_config_columns(column_names)
      dbconfig['column_names'] = column_names
      return None
    except Exception as e:
      emsg = f"Error executing json.load('{dbconfig['jsondb']['head']}')"
      if update:
        logger.error(f"{emsg}.")
        return emsg
      logger.error(f"{emsg}: {e}. Exiting.")
      exit(1)


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
          if update:
            logger.error(f"{emsg}.")
            return emsg
          logger.error(f"{emsg}: {e}. Exiting.")
          exit(1)

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
      if update:
        logger.error(f"{emsg}.")
        return emsg
      logger.error(f"{emsg}: {e}. Exiting.")
      exit(1)


def _sql_n_rows(dbconfig, update=False):
  try:
    query = f"SELECT COUNT(*) FROM `{dbconfig['table_name']}`"
    logger.info("Getting number of rows")
    dbconfig["n_rows"] = _sql_execute(query, sqldb=dbconfig['sqldb'])[0][0]
    logger.info(f"Got number of rows = {dbconfig['n_rows']}\n")
  except Exception as e:
    emsg = "Error getting number of rows"
    if update:
      logger.error(f"{emsg}.")
      return emsg
    logger.error(f"{emsg}: {e}. Exiting.")
    exit(1)

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
    if update:
      logger.error(f"{emsg}.")
      return emsg
    logger.error(f"{emsg}: {e}. Exiting.")
    exit(1)

  dbconfig['sqldb_tables'] = table_names

  return None
