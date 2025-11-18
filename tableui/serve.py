import os
import copy
import json
import time
import urllib
import logging

import sqlite3

logger = logging.getLogger(__name__)

ROOT_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
CONFIG_DEFAULT = os.path.join(ROOT_DIR, "conf", "default.json")
RENDER_DEFAULT = os.path.join(ROOT_DIR, "js", "render.js")
STYLE_DEFAULT = os.path.join(ROOT_DIR, "css", "index.css")


def app(config):
  import fastapi

  if isinstance(config, str):
    # Load configuration from JSON file
    with open(config, "r") as f:
      logger.info(f"Reading: {config}")
      config = f.read()
      config = json.loads(config)
      debug = config.get("debug", False)
      log_level = config.get("log_level", None)
      config = config['config']
      if debug:
        logger.setLevel(logging.DEBUG)
      if log_level is not None:
        logger.setLevel(log_level.upper())
      logger.info(f"Debug: {debug}, log_level: {log_level}")

  app = fastapi.FastAPI()
  _api_init(app, config)

  return app


def _api_init(app, config, path=None, related_paths=[]):

  import fastapi
  # Must import StaticFiles from fastapi.staticfiles
  # fastapi.staticfiles is not in dir(fastapi) (it is added dynamically)
  from fastapi.staticfiles import StaticFiles

  def parse_int(parameter, parameters, min=0, default=None):
    if parameter in parameters:
      val = parameters[parameter]
      try:
        val = int(val)
      except Exception:
        emsg = f"Error: {parameter} must be an integer"
        content = {"error": emsg}
        return fastapi.responses.JSONResponse(content=content, status_code=400)
      if not val >= min:
        emsg = f"Error: {parameter} >= {min} required"
    else:
      val = default

    parameters[parameter] = val

  if path is None:
    path_list, _ = _paths(config)
    related_paths, _ = _related_paths(config, path_list)
    if len(path_list) == 0:
      _api_init(app, config, path="")
    else:
      for path in path_list:
        _api_init(app, config, path=path, related_paths=related_paths)
    return

  logger.info(f"Initalizing API with base path = '{path}'")

  # Get config information. update=False => exit on error
  # _r => resolved (file paths, table names, column names, etc.)
  config_r, _ = _config_resolve(config, path=path, update=False)

  path_o = path
  if path != "":
    path = f"/{path.strip('/')}"

  for dir in ['js', 'css', 'demo', 'misc']:
    directory = os.path.join(ROOT_DIR, dir)
    app.mount(f"{path}/{dir}/", StaticFiles(directory=directory))

  path_list = []
  if len(related_paths) > 1:
    for related_path in related_paths:
      path_list.append(related_path['path'])
    if "" not in path_list:
      # If more than one path and no root path, redirect root to first path
      logger.info(f"Initalizing endpoint {path}/")
      @app.route("/", methods=["GET", "HEAD"])
      def rootredirect(request: fastapi.Request):
        target = path_list[0]
        if not target.startswith('/'):
          target = f"/{target}"
        url = f"{target}/"
        logger.info(f"Redirecting '/' to '{url}'")
        return fastapi.responses.RedirectResponse(url, status_code=302)

  logger.info(f"Initalizing endpoint {path}/")
  @app.route(f"{path}/", methods=["GET", "HEAD"])
  def indexhtml(request: fastapi.Request):
    # Silently ignores any query parameters
    fname = os.path.join(ROOT_DIR, 'index.html')
    logger.info("Reading: " + fname)
    with open(fname) as f:
      indexhtml_ = f.read()
    return fastapi.responses.HTMLResponse(indexhtml_)

  if "jsondb" in config_r:
    endpoint = f"{path}/jsondb"
    logger.info(f"Initalizing endpoint '{endpoint}'")
    @app.route(endpoint, methods=["GET", "HEAD"])
    def jsondb(request: fastapi.Request):
      # Silently ignores any query parameters other than _verbose
      query_params = dict(request.query_params)

      config_r, err = _config_resolve(config, path=path_o, update=True)
      if err is not None:
        content = {"error": err}
        return fastapi.responses.JSONResponse(content=content, status_code=500)

      with open(config_r['jsondb']['body']) as f:
        # TODO: Stream
        logger.info("Reading and sending: " + config_r['jsondb']['body'])
        data = json.load(f)
        # TODO: Use _verbose validation in data()
        data = _data_transform(data, config_r['column_names'], query_params.get("_verbose", None) == "true")
      data = {
        "columns": config_r['column_names'],
        "data": data
      }
      return fastapi.responses.JSONResponse(data)

  if "sqldb" in config_r and config_r["sqldb"] is not None:
    endpoint = f"{path}/sqldb"
    logger.info(f"Initalizing endpoint '{endpoint}'")
    @app.route(f"{path}/sqldb", methods=["GET", "HEAD"])
    def sqldb(request: fastapi.Request):
      # Silently ignores any query parameters

      config_r, err = _config_resolve(config, path=path_o, update=True)
      if err is not None:
        content = {"error": err}
        return fastapi.responses.JSONResponse(content=content, status_code=500)

      filename = os.path.basename(config_r['sqldb'])
      kwargs = {
                  'media_type': 'application/x-sqlite3',
                  'filename': filename
                }
      return fastapi.responses.FileResponse(config_r['sqldb'], **kwargs)

  endpoint = f"{path}/config"
  logger.info(f"Initalizing endpoint '{endpoint}'")
  @app.route(f"{path}/config", methods=["GET", "HEAD"])
  def configx(request: fastapi.Request):
    # Silently ignores any query parameters

    config_r, err = _config_resolve(config, path=path_o, update=True)
    if err is not None:
      content = {"error": err}
      return fastapi.responses.JSONResponse(content=content, status_code=500)

    related_paths, err = _related_paths(config, path_list, update=True)
    if err is not None:
      content = {"error": err}
      return fastapi.responses.JSONResponse(content=content, status_code=500)

    if len(related_paths) > 1:
      config_r['dataTablesAdditions']['relatedTables'] = related_paths

    content = {
      "dataTables": config_r['dataTables'],
      "dataTablesAdditions": config_r['dataTablesAdditions']
    }

    return fastapi.responses.JSONResponse(content=content)

  endpoint = f"{path}/style.css"
  logger.info(f"Initalizing endpoint '{endpoint}'")
  @app.route(f"{path}/style.css", methods=["GET", "HEAD"])
  def style(request: fastapi.Request):
    # Silently ignores any query parameters
    config_r, err = _config_resolve(config, path=path_o, update=True)
    if err is not None:
      content = {"error": err}
      return fastapi.responses.JSONResponse(content=content, status_code=500)

    content = _read_default('style', config_r)

    return fastapi.responses.Response(content=content, media_type="text/css")

  endpoint = f"{path}/render.js"
  logger.info(f"Initalizing endpoint '{endpoint}'")
  @app.route(f"{path}/render.js", methods=["GET", "HEAD"])
  def render(request: fastapi.Request):
    # Silently ignores any query parameters
    config_r, err = _config_resolve(config, path=path_o, update=True)
    if err is not None:
      content = {"error": err}
      return fastapi.responses.JSONResponse(content=content, status_code=500)

    media_type = "application/javascript"
    content = _read_default('renderFunctions', config_r)
    return fastapi.responses.Response(content=content, media_type=media_type)

  endpoint = f"{path}/data/"
  logger.info(f"Initalizing endpoint '{endpoint}'")
  @app.route(f"{path}/data/", methods=["POST", "GET", "HEAD"])
  def data(request: fastapi.Request):

    logger.info(f"Data request with query params: {request.query_params}")
    query_params = dict(request.query_params)

    if "_verbose" in query_params:
      if query_params["_verbose"] not in ["true", "false"]:
        emsg = "Error: _verbose must be 'true' or 'false'"
        content = {"error": emsg}
        return fastapi.responses.JSONResponse(content=content, status_code=400)
      if query_params["_verbose"] == "true":
        query_params["_verbose"] = True
      else:
        query_params["_verbose"] = False
    else:
      query_params["_verbose"] = False

    config_r, err = _config_resolve(config, path=path_o, update=True)
    if err is not None:
      content = {"error": err}
      return fastapi.responses.JSONResponse(content=content, status_code=500)

    if "jsondb" in config_r:
      # No server-side processing. Serve entire JSON and return.
      for key in query_params.keys():
        if key not in ["_", "_verbose"]:
          emsg = f"Error: Unknown query parameter: {key}. Only '_' and "
          emsg += "'_verbose' allowed when using jsondb (=> serverSide=false)."
          content = {"error": emsg}
          return fastapi.responses.JSONResponse(content=content, status_code=400)

      data = config_r['jsondb']['data']
      column_names = config_r['column_names']
      data = _data_transform(data, column_names, query_params["_verbose"])
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
      if key not in keys_allowed and key not in config_r['column_names']:
        logger.error(f"Unknown query parameter: {key}.")
        emsg = "Unknown query parameter with first five character of "
        emsg += f"{key[0:5]}. Allowed: {keys_allowed} and column names: "
        emsg += f"{config_r['column_names']}"
        content = {"error": emsg}
        return fastapi.responses.JSONResponse(content=content, status_code=400)

    if "_uniques" in query_params:
      if query_params["_uniques"] not in ["true", "false"]:
        emsg = "Error: _uniques must be 'true' or 'false'"
        content = {"error": emsg}
        return fastapi.responses.JSONResponse(content=content, status_code=400)
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
        if col not in config_r['column_names']:
          emsg = f"Error: _orders column '{col}' not found in column names: "
          emsg += f"{config_r['column_names']}"
          content = {"error": emsg}
          return fastapi.responses.JSONResponse(content=content, status_code=400)
      query_params["_orders"] = orders
    else:
      query_params["_orders"] = None

    searches = {}
    if query_params is not None:
      logger.info(f"Query params: {query_params}")
      for key, _ in query_params.items():
        if key in config_r['column_names']:
          kwargs = {'encoding': 'utf-8', 'errors': 'replace'}
          searches[key] = urllib.parse.unquote(query_params[key], **kwargs)
      query_params['searches'] = searches
      logger.info(f"Search params: {searches}")
    else:
      query_params['searches'] = None

    return_cols = config_r['column_names']
    if "_return" in query_params:
      query_params["_return"] = query_params["_return"].split(",")
      for col in query_params["_return"]:
        if col not in config_r['column_names']:
          emsg = f"Error: _return column '{col}' not found in column names: "
          emsg += f"{config_r['column_names']}"
          content = {"error": emsg}
          return fastapi.responses.JSONResponse(content=content, status_code=400)
      return_cols = query_params["_return"]
    else:
      query_params["_return"] = None

    try:
      result = _sql_query(config_r, query_params)
    except Exception as e:
      emsg = f"Error querying database: {e}"
      content = {"error": emsg}
      return fastapi.responses.JSONResponse(content=content, status_code=500)

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


def _paths(configs, update=False):
  if isinstance(configs, str):
     configs, eobj = _config_read(configs, update=update)
     if eobj is not None:
        return None, eobj

  paths = []
  for config in configs:
    if 'path' in config:
      paths.append(config['path'])

  # Ensure 'path' values in paths are unique.
  if len(paths) != len(set(paths)):
    emsg = f"Duplicate 'path' values detected in config; paths = {paths}"
    return None, _error(emsg, None, update=update)

  return paths, None


def _related_paths(config, path_list, update=False):
  related_paths = []
  for path in path_list:
    config_r, eobj = _config_resolve(config, path=path)
    if eobj is not None:
      return None, eobj
    # Server restart required to update related_paths
    related_path = {
      'path': path,
      'name': config_r['dataTablesAdditions']['tableMetadata']['tableName'],
      'title': config_r['dataTablesAdditions']['tableMetadata']['tableTitle']
    }
    related_paths.append(related_path)
  return related_paths, None


def _config_read(config_file, update=False):
  with open(config_file) as f:
    logger.info(f"Reading: {config_file}")
    try:
      configs = json.load(f)
      if isinstance(configs, dict):
        configs = [configs]
      return configs, None
    except Exception as e:
      emsg = f"Error executing json.load('{config_file}')"
      return None, _error(emsg, e, update)


def _config_resolve(config, path=None, update=False):

  if isinstance(config, str):
    base_dir = os.path.abspath(os.path.dirname(config))
    base_dir = os.path.normpath(base_dir)
    logger.info(f"Base path for relative paths in config: '{base_dir}'")
    configs, eobj = _config_read(config, update=update)
    if eobj is not None:
      return None, eobj
    for config in configs:
      config['base_dir'] = base_dir
  else:
    configs = copy.deepcopy(config)
    if not isinstance(configs, list):
      configs = [configs]

  # Endpoint paths
  paths, eobj = _paths(configs, update=update)
  if eobj is not None:
    return None, eobj

  if len(paths) > 1:
    found = False
    for config in configs:
      config['paths'] = paths
      if config.get('path', None) == path:
        found = True
        break
    if not found:
      emsg = f"Could not find config for path '{path}'"
      return None, _error(emsg, "", update)
  else:
    config = configs[0]

  # Directory paths
  eobj = _dir_resolve(config, update=update)
  if eobj is not None:
    return None, eobj

  if not update:
    if 'sqldb' not in config and 'jsondb' not in config:
      logger.error("Must specify at least sqldb or jsondb. Exiting.")
      exit(1)

    if 'sqldb' in config and 'jsondb' in config:
      logger.error("Both sqldb and jsondb were given. Choose one. Exiting.")
      exit(1)

  config['path'] = config.get('path', '')

  # Add 'table_meta' to config (default table metadata)
  eobj = _table_meta(config, update=update)
  if eobj is not None:
    return None, eobj

  # Add (or updates) and checks 'dataTables' in config
  emsg = _dataTables(config, update=update)
  if emsg is not None:
    return None, emsg

  if 'jsondb' in config:

    if not os.path.exists(config['jsondb']['body']):
      emsg = f"File not found: {config['jsondb']['body']}"
      return None, _error(emsg, "", update)

    try:
      with open(config['jsondb']['body']) as f:
        logger.info(f"Reading: {config['jsondb']['body']}")
        config['jsondb']['data'] = json.load(f)
    except Exception as e:
      emsg = f"Error reading jsondb body file {config['jsondb']['body']}"
      return None, _error(emsg, e, update)

    if config.get('table_name', None) is None:
      fname = config['jsondb']['body']
      config['table_name'] = os.path.basename(fname).split(".")[0]
      wmsg = f"No table name given; using '{config['table_name']}', "
      wmsg += "which is based on file name of json_body"
      logger.warning(wmsg)


    # Adds 'column_names' to config and 'columns' to config['dataTables']
    eobj = _column_names(config, update=update)
    if eobj is not None:
      return None, eobj

    # Adds or updates 'dataTablesAdditions' in config
    eobj = _dataTablesAdditions(config, update=update)
    if eobj is not None:
      return None, eobj

    return config, None


  # Adds 'sqldb_tables' to config
  eobj = _sql_table_names(config, update=update)
  if eobj is not None:
    return None, eobj

  if config.get('table_name', None) is None:
    table_name = ".".join(os.path.basename(config['sqldb']).split(".")[0:-1])
    if table_name in config['sqldb_tables']:
      msg = "No table_name given; Found table with name based on sqldb "
      msg += "file name: '{table_name}'"
      logger.info(msg)
      config['table_name'] = table_name
    else:
      emsg = "No table_name given and could not find table named "
      emsg += f"'{table_name}' in {config['sqldb']}"
      return None, _error(emsg, "", update)
  else:
    if config['table_name'] not in config['sqldb_tables']:
      emsg = f"Could not find table named '{config['table_name']}' in "
      emsg += f"{config['sqldb']}. Tables found: {config['sqldb_tables']}"
      return None, _error(emsg, "", update)

  # Adds 'column_names' to config and 'columns' to config['dataTables']
  emsg = _column_names(config, update=update)
  if emsg is not None:
    return None, emsg

  # Adds or updates 'dataTablesAdditions' in config
  eobj = _dataTablesAdditions(config, update=update)
  if eobj is not None:
    return None, eobj

  # Adds 'n_rows' to config
  eobj = _sql_n_rows(config, update=update)
  if eobj is not None:
    return None, eobj

  # Adds or updates 'dataTablesAdditions' in config
  eobj = _sql_table_meta(config, update=update)
  if eobj is not None:
    return None, eobj

  return config, None


def _dir_resolve(config, update=False):

  def expand_path(base_dir, path):

    if not isinstance(path, str):
      return path, None

    if path.startswith('~'):
      path_rel = path
      path = os.path.expanduser(path)
      if not os.path.exists(path):
        emsg = f"Converted path {path_rel} in config to absolute path {path}, but"
        emsg += f"'{path}' does not exist."
        return None, _error(emsg, "", update)

    if not os.path.isabs(path):
      path_rel = path
      path = os.path.join(base_dir, path)
      path = os.path.normpath(path)
      if not os.path.exists(path):
        emsg = f"Converted relative path '{path_rel}' to absolute path using "
        emsg += f"base path = '{base_dir}' giving '{path}', but file does not exist."
        return None, _error(emsg, "", update)

    return path, None

  base_dir = config.get('base_dir', os.getcwd())
  if 'sqldb' in config:
    config['sqldb'], eobj = expand_path(base_dir, config['sqldb'])
    if eobj is not None:
      return eobj
  if 'jsondb' in config:
    if isinstance(config['jsondb'], str):
      config['jsondb'], eobj = expand_path(base_dir, config['jsondb'])
      if eobj is not None:
        return eobj
    if 'body' in config['jsondb']:
      config['jsondb']['body'], eobj = expand_path(base_dir, config['jsondb']['body'])
      if eobj is not None:
        return eobj
    if 'head' in config['jsondb']:
      config['jsondb']['head'], eobj = expand_path(base_dir, config['jsondb']['head'])
      if eobj is not None:
        return eobj
  if 'dataTables' in config:
    config['dataTables'], eobj = expand_path(base_dir, config['dataTables'])
    if eobj is not None:
      return eobj

  if 'dataTablesAdditions' in config:
    if 'renderFunctions' in config['dataTablesAdditions']:
      path = config['dataTablesAdditions']['renderFunctions']
      config['dataTablesAdditions']['renderFunctions'], eobj = expand_path(base_dir, path)
      if eobj is not None:
        return eobj
    if 'style' in config['dataTablesAdditions']:
      path = config['dataTablesAdditions']['style']
      config['dataTablesAdditions']['style'], eobj = expand_path(base_dir, path)
      if eobj is not None:
        return eobj


def _dataTablesAdditions(config, update=False):

  dataTablesAdditions = config.get("dataTablesAdditions", {})

  if 'renderFunctions' in dataTablesAdditions:
    if not os.path.exists(dataTablesAdditions['renderFunctions']):
      emsg = f"File not found: {dataTablesAdditions['renderFunctions']}"
      return _error(emsg, "", update)

  # Merge table_meta from config and dataTablesAdditions
  table_meta = copy.deepcopy(config.get('table_meta', None))
  if table_meta is not None:
    # Overwrite any existing keys in config['table_meta'] (defaults)
    # with those in tableMetadata
    table_meta = {**table_meta, **dataTablesAdditions.get('tableMetadata', {})}
    dataTablesAdditions["tableMetadata"] = table_meta

  if dataTablesAdditions.get('tableMetadata', None) is None:
    dataTablesAdditions['tableMetadata'] = {}

  dataTablesAdditions['tableMetadata']['tableName'] = config['table_name']
  if dataTablesAdditions['tableMetadata'].get('tableTitle', None) is None:
    dataTablesAdditions['tableMetadata']['tableTitle'] = config['table_name']

  if "sqldb" in config:
    dbfile = config["sqldb"]
    #dataTablesAdditions['sqldb'] = os.path.basename(dbfile)
    dataTablesAdditions['tableMetadata']['tableType'] = "sqlite3"
    dataTablesAdditions['tableMetadata']['tableFile'] = os.path.basename(dbfile)

  if "jsondb" in config:
    dbfile = config["jsondb"]["body"]
    #dataTablesAdditions['jsondb'] = os.path.basename(dbfile)
    dataTablesAdditions['tableMetadata']['tableType'] = "json"
    dataTablesAdditions['tableMetadata']['tableFile'] = os.path.basename(dbfile)

  if dataTablesAdditions['tableMetadata'].get('creationDate', None) is None:
    try:
      import datetime
      mtime = os.path.getmtime(dbfile)
      creationDate = datetime.datetime.fromtimestamp(mtime).isoformat()
      dataTablesAdditions['tableMetadata']['creationDate'] = creationDate[0:-7] + "Z"
    except Exception as e:
      logger.warning(f"Could not get file modification time for {dbfile}: {e}")

  config['dataTablesAdditions'] = dataTablesAdditions


def _dataTables(config, update=False):

  if 'dataTables' not in config:
    config['dataTables'] = CONFIG_DEFAULT

  config_file = config.get('config_file', None)
  if isinstance(config['dataTables'], str) or config_file is not None:
    if isinstance(config['dataTables'], str):
      config_file = config['dataTables']
      config['config_file'] = config_file
    with open(config_file) as f:
      logger.info("Reading: " + config_file)
      try:
        config['dataTables'] = json.load(f)
      except Exception as e:
        emsg = f"Error executing json.load('{config_file}')"
        return _error(emsg, e, update)

  serverSide = config['dataTables'].get('serverSide', None)

  if serverSide is None or serverSide not in [True, False]:
    if 'sqldb' in config:
      config['dataTables']['serverSide'] = True
    if 'jsondb' in config:
      config['dataTables']['serverSide'] = False
  else:
    if 'sqldb' in config and not serverSide:
      if update:
        logger.warning("Warning: Config file specifies serverSide=false but input is sqldb. Overriding to serverSide=true")
      config['dataTables']['serverSide'] = True
    if 'jsondb' in config and serverSide:
      if update:
        logger.warning("Warning: Config file specifies serverSide=true but jsondb was given. Overriding to serverSide=false")
      config['dataTables']['serverSide'] = False


def _error(emsg, err, update):
  if update:
    logger.error(f"{emsg}.")
    return emsg
  if err:
    err = f"{emsg}: {err}"
  if emsg.endswith('.'):
    emsg = emsg[0:-1]
  logger.error(f"{emsg}. Exiting.")
  exit(1)


def _data_transform(data, column_names, verbose):

  if not verbose:
    return data
  data_verbose = []
  logger.debug(f"Transforming data to verbose format. Column names: {column_names}")
  for row in data:
    data_verbose.append({column_names[i]: row[i] for i in range(len(column_names))})
  return data_verbose


def _column_names(config, update=False):

  def set_config_columns(column_names):
    config['dataTables']['columns'] = []
    for column_name in column_names:
      config['dataTables']['columns'].append({"name": column_name})

  if 'sqldb' in config:
    query = f"PRAGMA table_info('{config['table_name']}');"
    try:
      logger.info("Getting column names")
      data = _sql_execute(query, sqldb=config['sqldb'])
      logger.info(f"Got {len(data)} column names\n")
      config['column_names'] = [row[1] for row in data]
    except Exception as e:
      emsg = "Error getting column names"
      return _error(emsg, e, update)

    set_config_columns(config['column_names'])

    return None

  n_rows = len(config['jsondb']['data'][0])
  column_names = [str(c) for c in range(0, n_rows)]
  if config['jsondb'].get('head', None) is None:
    if not update:
      logger.warning("No json_head file given. Using indices for column names.")
    set_config_columns(column_names)
    config['column_names'] = column_names
    return None

  if not os.path.exists(config['jsondb']['head']):
    emsg = f"File not found: {config['jsondb']['head']}"
    _error(emsg, "", update)

  with open(config['jsondb']['head']) as f:
    try:
      column_names = json.load(f)
      set_config_columns(column_names)
      config['column_names'] = column_names
      return None
    except Exception as e:
      emsg = f"Error executing json.load('{config['jsondb']['head']}')"
      return _error(emsg, e, update)


def _read_default(which, config_r):
  if which == 'renderFunctions':
    default_file = RENDER_DEFAULT
    content_type = "javascript"
  if which == 'style':
    default_file = STYLE_DEFAULT
    content_type = "css"

  default = ""
  with open(default_file) as f:
    default = f.read()

  content = f"/* Default {content_type} */\n"
  if which in config_r['dataTablesAdditions']:
    user = config_r['dataTablesAdditions'][which]
    try:
      with open(user) as f:
        user = f.read()
      content += default + f"\n/* User defined {content_type} */\n" + user
    except Exception as e:
      logger.error(e)
      user = config_r['dataTablesAdditions'][which]
      try:
        user = os.path.basename(user)
      except Exception:
        pass
      content += default
      content += f"\n// Could not read {content_type} file '{user}' in config.dataTablesAdditions\n"
  else:
    content += default

  return content


def _table_meta(config, update=False):

  if config.get('table_meta', None) is None:
    if 'jsondb' in config:
      if not update:
        logger.warning("No table_meta file given.")
  else:
    if isinstance(config['table_meta'], str) and os.path.exists(config['table_meta']):
      with open(config['table_meta']) as f:
        try:
          config['table_meta'] = json.load(f)
        except Exception as e:
          emsg = f"Error executing json.load('{config['table_meta']}')"
          return _error(emsg, e, update)

  return None


def _sql_cursor(sqldb, memory=False):
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


def _sql_execute(query, sqldb=None, params=None):

  start = time.time()
  cursor, connection = _sql_cursor(sqldb, memory=False)

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
        elif val.startswith('>'):
            where.append(f"`{key}` > ?")
            params.append(val[1:])
        elif val.startswith('≥'):
            where.append(f"`{key}` >= ?")
            params.append(val[1:])
        elif val.startswith('<'):
            where.append(f"`{key}` < ?")
            params.append(val[1:])
        elif val.startswith('≤'):
            where.append(f"`{key}` <= ?")
            params.append(val[1:])
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
      query = f"SELECT `{col}`, COUNT(*) as count FROM `{dbinfo['table_name']}` "
      query += f"{clause_str} GROUP BY `{col}`"
      rows = _sql_execute(query, sqldb=dbinfo['sqldb'], params=clause_params)
      logger.info(f"Got {len(rows)} unique values\n")
      # Each value is a list of (value, count) tuples
      uniques[col] = rows.copy()
    return {"data": uniques}

  if _return is None:
    columns_str = "*"
  else:
    columns_str = ", ".join([f"`{col}`" for col in _return])

  query = f"SELECT {columns_str} FROM `{dbinfo['table_name']}` "
  query += f"{clause_str} {orderby(orders)}"
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
    recordsFiltered = _sql_execute(query_count,
                                   sqldb=dbinfo['sqldb'],
                                   params=clause_params)[0][0]
    logger.info(f"Got number of filtered records = {recordsFiltered}\n")

  if limit is None:
    limit = recordsTotal

  warning = None
  if offset >= recordsFiltered:
    warning = f"_start={offset} is larger than the number of filtered records "
    warning += f" ({recordsFiltered}). Setting _start to "
    warning += f"{max(0, recordsFiltered - limit)}"
    offset = max(0, recordsFiltered - limit)

  if offset + limit > recordsFiltered:
    warning = f"_start + _length = {offset + limit} is larger than the "
    warning += f"number of filtered records ({recordsFiltered})."
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


def _sql_table_meta(config, update=False):

  table_metadata = f"{config['table_name']}.metadata"
  if f'{table_metadata}' in config['sqldb_tables']:
    if 'table_meta' in config and config['table_meta'] is not None:
      if update:
        wmsg = "Warning: table_meta already given and will be over-written "
        wmsg += f"by {table_metadata} table in {config['sqldb']}"
        logger.warning(wmsg)
    try:
      query = f"SELECT * FROM `{table_metadata}`"
      logger.info("Getting table metadata")
      row = _sql_execute(query, sqldb=config['sqldb'])[0]
      if row is not None:
        config['table_meta'] = json.loads(row[1])
        keys = list(config['table_meta'].keys())
        logger.info(f"Got table metadata with keys: {keys}\n")
      else:
        logger.info("No table metadata\n")
        return None
    except Exception as e:
      emsg = "Error getting table metadata"
      return _error(emsg, e, update)


def _sql_n_rows(config, update=False):
  try:
    query = f"SELECT COUNT(*) FROM `{config['table_name']}`"
    logger.info("Getting number of rows")
    config["n_rows"] = _sql_execute(query, sqldb=config['sqldb'])[0][0]
    logger.info(f"Got number of rows = {config['n_rows']}\n")
  except Exception as e:
    emsg = f"Error getting number of rows in {config['table_name']}"
    return _error(emsg, e, update)

  return None


def _sql_table_names(config, update=False):

  query = "SELECT name FROM sqlite_master WHERE type='table';"
  try:
    table_names = []
    logger.info("Getting table names")
    data = _sql_execute(query, sqldb=config['sqldb'])
    for row in data:
      table_names.append(row[0])
    logger.info("Got tables names")
    logger.info(f"  {table_names}\n")
  except Exception as e:
    emsg = f"Error getting table names from {config['sqldb']}"
    return _error(emsg, e, update)

  config['sqldb_tables'] = table_names

  return None
