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
logging.basicConfig(level=logging.INFO)

root_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
def serve(sqldb=None, table_name=None,
          json_head=None, json_body=None,
          host="0.0.0.0", port=5001, root_dir=root_dir,
          config="config.json", render="js/render.js"):

  kwargs = tableui.cli.defaults(locals())

  logger.info(f"Getting database info in file '{kwargs['sqldb']}'")
  _kwargs = {
    "sqldb": kwargs['sqldb'],
    "table_name": kwargs['table_name'],
    "json_head": kwargs['json_head'],
    "json_body": kwargs['json_body']
  }
  dbinfo = _dbinfo(**_kwargs)

  app = fastapi.FastAPI()

  logger.info("Initalizing API")
  _kwargs = {
    "root_dir": kwargs['root_dir'],
    "dbinfo": dbinfo,
    "dtrender": kwargs['render'],
    "dtconfig": kwargs['config']
  }
  _api_init(app, **_kwargs)

  logger.info("Starting server")
  _kwargs = {
              "host": kwargs['host'],
              "port": kwargs['port'],
              "server_header": False
            }
  uvicorn.run(app, **_kwargs)

def _api_init(app, root_dir=None, dbinfo=None, dtrender=None, dtconfig=None):


  def cors_headers(response: fastapi.Response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, HEAD, OPTIONS"
    return response

  # Must import StaticFiles from fastapi.staticfiles
  # fastapi.staticfiles is not in dir(fastapi) (it is added dynamically)
  from fastapi.staticfiles import StaticFiles
  directory = os.path.join(root_dir, 'js')
  app.mount("/js", StaticFiles(directory=directory))
  directory = os.path.join(root_dir, 'css')
  app.mount("/css", StaticFiles(directory=directory))
  directory = os.path.join(root_dir, 'img')
  app.mount("/img", StaticFiles(directory=directory))

  @app.route("/", methods=["GET", "HEAD"])
  def indexhtml(request: fastapi.Request):
    fname = os.path.join(root_dir,'index.html')
    logger.info("Reading: " + fname)
    with open(fname) as f:
      indexhtml_ = f.read()
    indexhtml_ = indexhtml_.replace("__TABLENAME__", dbinfo['table_name'])
    return fastapi.responses.HTMLResponse(indexhtml_)

  @app.route("/config", methods=["GET", "HEAD"])
  def config(request: fastapi.Request):
    with open(dtconfig) as f:
      logger.info("Reading: " + dtconfig)
      content = json.load(f)

    if 'sqldb' in dbinfo:
      content["serverSide"] = True

    return fastapi.responses.JSONResponse(content=content)

  @app.route("/render.js", methods=["GET", "HEAD"])
  def render(request: fastapi.Request):
    return fastapi.responses.FileResponse(dtrender)

  @app.route("/header", methods=["GET", "HEAD"])
  def header(request: fastapi.Request):
    return fastapi.responses.JSONResponse(content=dbinfo['column_names'])

  @app.route("/data/", methods=["GET", "HEAD"])
  def data(request: fastapi.Request):

    query_params = dict(request.query_params)

    if "_start" not in query_params:
      # No server-side processing. Serve entire JSON.
      with open(dbinfo['jsondb']) as f:
        # TODO: Stream
        logger.info("Reading: " + dbinfo['jsondb'])
        data = json.load(f)
      return fastapi.responses.JSONResponse({"data": data})


    result = _query(dbinfo, query_params=query_params)
    content = {
                "draw": query_params["_draw"],
                "recordsTotal": result['recordsTotal'],
                "recordsFiltered": result['recordsFiltered'],
                "data": result['data']
              }

    return fastapi.responses.JSONResponse(content=content)


def _column_names(sqldb=None, table_name=None, json_head=None, json_body=None):

  if sqldb is not None:
    connection = sqlite3.connect(sqldb)
    cursor = connection.cursor()
    query_ = f"select * from '{table_name}';"
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

  if json_body and json_head is None:
    return list(range(0, len(json_body[0])))

  with open(json_head) as f:
    try:
      header = json.load(f)
    except:
      header = list(range(0, len(json_body[0])))
    return header

def _table_names(sqldb=None):
  connection = sqlite3.connect(sqldb)

  query_ = "SELECT name FROM sqlite_master WHERE type='table';"
  try:
    cursor = connection.cursor()
    cursor.execute(query_)
    table_names = list(cursor.fetchall()[0])
  except Exception as e:
    print(f"Error executing query for table names using '{query_}' on {sqldb}")
    raise e
  logger.info(f"Tables in {sqldb}: {table_names}")

  connection.close()

  return table_names


def _query(dbinfo, query_params=None):

  start = 0
  limit = None
  if query_params is not None and "_start" in query_params:
    start = int(query_params["_start"])
    end = int(query_params["_start"]) + int(query_params["_length"])
    limit = end - start

  orders = None
  if "_orders" in query_params:
    orders = query_params["_orders"].split(",")

  searches = {}
  if query_params is not None:
    print("Query params: ", query_params)
    for key, _ in query_params.items():
      if key in dbinfo['column_names']:
        searches[key] = urllib.parse.unquote(query_params[key], encoding='utf-8', errors='replace')

  logger.info("Connecting to database file " + dbinfo['sqldb'])
  result = _dbquery(dbinfo, orders=orders, searches=searches, limit=limit, offset=start)
  return result

def _dbinfo(sqldb=None, table_name=None, json_head=None, json_body=None):

  dbinfo = {
    "sqldb": None,
    "jsondb": None,
    "table_name": None,
    "n_rows": None,
    "column_names": None
  }

  if sqldb is None:
    del dbinfo["sqldb"]
    if table_name is None:
      table_name = os.path.basename(json_body).split(".")[0]
      logger.info(f"No table name given; using '{table_name}'")
    dbinfo["table_name"] = table_name
    dbinfo["jsondb"] = json_body
    dbinfo["column_names"] = _column_names(json_head=json_head, json_body=json_body)

    return dbinfo

  if table_name is None:
    del dbinfo["jsondb"]
    table_names = _table_names(sqldb=sqldb)
    table_name = ".".join(os.path.basename(sqldb).split(".")[0:-1])
    if table_name in table_names:
      logger.info(f"No table name given; using table name based on sqldb file name: '{table_name}'")
    else:
      logger.info(f"No table name given; using first table returned from list of table names: '{table_name}'")
      table_name = table_names[0]

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

def _dbquery(dbinfo, orders=None, searches=None, limit=None, offset=0):

  conn = sqlite3.connect(dbinfo['sqldb'])
  cursor = conn.cursor()

  def execute(query):
    start = time.time()
    logger.info(f"Executing {query}")
    result = cursor.execute(query)
    data = result.fetchall()
    dt = "{:.4f} [s]".format(time.time() - start)
    logger.info(f"Took {dt} to query and fetch")
    return data

  def _n_rows_filtered(clause):
    logger.info("Counting # of rows after applying search filters")
    query = f"SELECT COUNT(*) FROM `{dbinfo['table_name']}` {clause}"
    return execute(query)[0]

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
    for key in keys:
      if searches[key] == "''" or searches[key] == '""':
        where.append(f" `{key}` = ''")
      elif searches[key].startswith("'") and searches[key].endswith("'"):
        where.append(f" `{key}` = {searches[key]}")
      else:
        where.append(f" `{key}` LIKE '%{searches[key]}%'")
    if len(where) == 0:
      return ""
    return "WHERE" + " AND ".join(where)

  n_rows = dbinfo['n_rows']

  query = f"SELECT * FROM `{dbinfo['table_name']}` {clause(searches)} {orderby(orders)}"
  if offset == 0 and limit is None:
    logger.info("Executing query with no limit and offset")
    data = execute(query)
    n_filtered = dbinfo['n_rows']
    if searches is not None:
      n_filtered = _n_rows_filtered(clause(searches))
    return {'recordsTotal': n_rows, 'recordsFiltered': n_filtered, 'data': data}

  n_filtered = n_rows
  if searches is not None:
    n_filtered = _n_rows_filtered(clause(searches))

  if limit is None:
    limit = n_rows

  logger.info("Executing query with limit and offset")
  query = f"{query} LIMIT {limit} OFFSET {offset}"
  data = execute(query)

  conn.close()

  return {'recordsTotal': n_rows, 'recordsFiltered': n_filtered, 'data': data}
