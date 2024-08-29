import os
import sys
import time
import json
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

try:
  import uvicorn
  import fastapi
  import sqlite3
except:
  print(os.popen('pip install uvicorn fastapi sqlite3').read())

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import Response, JSONResponse, HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles


def serve(args):
  checkdb(args)

  app = FastAPI()
  api_init(app, args)

  kwargs = {
              'host': args['host'],
              'port': args['port'],
              'server_header': False
            }
  uvicorn.run(app, **kwargs)


def cli():

  clkws = {
    "id": {
      "help": "ID or pattern for dataset IDs to include (prefix with ^ to use pattern match, e.g., '^A|^B') (default: ^.*)"
    },
    "root-dir": {
      "help": "Root directory with subdirs of js, css, and img",
      "default": os.path.dirname(__file__)
    },
    "sqldb": {
      "help": "File containing SQL database",
      "default": None
    },
    "table": {
      "help": "Name of table in sqldb. Defaults to file name without extension of sqldb",
      "default": None
    },
    "config": {
      "help": "JSON file containing configuration. See https://datatables.net/reference/option/ for options.",
      "default": os.path.join(os.path.dirname(__file__), 'config.json')
    },
    "render": {
      "help": "Javascript file with DataTables rendering function. Relative paths are relative to root-dir",
      "default": 'js/render.js'
    },
    "port": {
      "help": "Serve table as a web page at http://localhost:port. Must specify --table_name",
      "type": int,
      "default": 5001
    },
    "host": {
      "help": "Serve table as a web page at http://localhost:port. Must specify --table_name",
      "default": "0.0.0.0"
    },
    "json-head": {
      "metavar": "FILE",
      "help": "JSON file containing array of header names. Ignored if sqldb given. Requres --json-body to be given.",
      "default": None
    },
    "json-body": {
      "metavar": "FILE",
      "help": "JSON array with (row) arrays of length header. Ignored if sqldb given. Requires --json-header to be given.",
      "default": None
    }
  }

  import argparse
  parser = argparse.ArgumentParser()
  for k, v in clkws.items():
    parser.add_argument(f'--{k}', **v)

  args = vars(parser.parse_args())

  if not os.path.isabs(args['render']):
    args['render'] = os.path.join(args['root_dir'], args['render'])

  if not args['sqldb'] and not args['json_head'] and not args['json_body']:
    print("ERROR: Must specify --sqldb or --json-header and --json-body")
    exit(1)

  if args['sqldb'] is None:
    if not args['json_head'] or not args['json_body']:
      print("ERROR: Must specify --json-header and --json-body if not using --sqldb")
      exit(1)

    file_head = args['json_head']
    if file_head is None:
      print("WARNING: No json-head file given. Using indices as column names.")
    else:
      if not os.path.exists(file_head):
        args['json_head'] = None
        print("WARNING: File not found: " + file_head + ". Using indices as column names.")

    json_body = args['json_body']
    if not os.path.exists(json_body):
      print("ERROR: File not found: " + json_body)
      exit(1)
  else:

    if args['table'] is None:
      args['table'] = os.path.splitext(os.path.basename(args['sqldb']))[0]
      # TODO: Check if table exists in database
      pass

  return args


def column_names(args):

  if args['sqldb'] is not None:
    connection = sqlite3.connect(args['sqldb'])
    cursor = connection.cursor()
    query_ = f"select * from '{args['table']}';"
    try:
      logger.info(f"Executing {query_}")
      cursor.execute(query_)
      connection.close()
      return [description[0] for description in cursor.description]
    except Exception as e:
      print(f"Error executing query for column names using '{query_}' on {args['sqldb']}")
      raise e

  if args['json_body'] and args['json_head'] is None:
    return list(range(0, len(args['json_body'][0])))

  with open(args['json_head']) as f:
    try:
      header = json.load(f)
    except:
      header = list(range(0, len(args['json_body'][0])))
    return header


def checkdb(args):

  if args['sqldb'] is None:
    return

  import sqlite3
  connection = sqlite3.connect(args['sqldb'])

  query_ = "SELECT name FROM sqlite_master WHERE type='table';"
  try:
    cursor = connection.cursor()
    cursor.execute(query_)
    table_names = list(cursor.fetchall()[0])
  except Exception as e:
    print(f"Error executing query for table names using '{query_}' on {args['sqldb']}")
    raise e
  logger.info(f"Tables in {args['sqldb']}: {table_names}")

  connection.close()

  logger.info(f"Found {len(column_names(args))} columns")


def querydb(conn, table, query="", orders=None, searches=None, offset=0, limit=None):

  cursor = conn.cursor()

  def execute(query):
    start = time.time()
    logger.info(f"Executing {query}")
    result = cursor.execute(query)
    data = result.fetchall()
    dt = "{:.4f} [s]".format(time.time() - start)
    logger.info(f"Took {dt} to query and fetch")
    return data

  def ntotal():
    # Could cache this result
    query = f"SELECT COUNT(*) FROM `{table}`"
    return execute(query)[0]

  def nfiltered(clause):
    query = f"SELECT COUNT(*) FROM `{table}` {clause}"
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
    logger.info(f"Search: {searches}")
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

  query = f"SELECT * FROM `{table}` {query} {clause(searches)} {orderby(orders)}"

  if offset == 0 and limit == None:
    data = execute(query)
    total = len(data)
    filtered = total
    if searches is not None:
      total = ntotal()
      filtered = nfiltered(clause(searches))
    return data, total, filtered

  total = ntotal()
  filtered = total
  if searches is not None:
    filtered = nfiltered(clause(searches))

  if limit is None:
    limit = total

  query = f"{query} LIMIT {limit} OFFSET {offset}"
  data = execute(query)

  return data, total, filtered


def query(args, query_params=None):

  start = None
  if query_params is not None and "_start" in query_params:
    start = int(query_params["_start"])
    end = int(query_params["_start"]) + int(query_params["_length"])

  if args['sqldb'] is None:
    with open(args['json_body']) as f:
      # TODO: Stream
      print("Reading: " + args['json_body'])
      DATA = json.load(f)
      if start is None:
        return DATA, len(DATA), len(DATA)
      else:
        return DATA[start:end], len(DATA), len(DATA)
  else:

    orders = None
    if "_orders" in query_params:
      orders = query_params["_orders"].split(",")

    import urllib
    searches = {}
    for key, value in query_params.items():
      if key in column_names(args):
        #searches[key] = query_params[key]
        searches[key] = urllib.parse.unquote(query_params[key], encoding='utf-8', errors='replace')

    logger.info("Connecting to database file " + args['sqldb'])
    conn = sqlite3.connect(args['sqldb'])
    DATA, ntotal, nfiltered = querydb(conn, args['table'], orders=orders, searches=searches, offset=start, limit=end-start)
    conn.close()
    return DATA, ntotal, nfiltered


def api_init(app, args):

  def cors_headers(response: Response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, HEAD, OPTIONS"
    return response

  # Serve static files
  directory = os.path.join(args['root_dir'], 'js')
  app.mount("/js", StaticFiles(directory=directory))
  directory = os.path.join(args['root_dir'], 'css')
  app.mount("/js", StaticFiles(directory=directory))
  directory = os.path.join(args['root_dir'], 'img')
  app.mount("/img", StaticFiles(directory=directory))

  @app.route("/", methods=["GET", "HEAD"])
  def indexhtml(request: Request):
    fname = os.path.join(os.path.dirname(__file__),'index.html')
    logger.info("Reading: " + fname)
    with open(fname) as f:
      indexhtml_ = f.read()
      return HTMLResponse(indexhtml_.replace("__TABLENAME__", args['table']))

  @app.route("/config", methods=["GET", "HEAD"])
  def config(request: Request):
    with open(args['config']) as f:
      logger.info("Reading: " + args['config'])
      config_ = json.load(f)

    if args['sqldb'] is not None:
      config_["serverSide"] = True

    return JSONResponse(content=config_)

  @app.route("/render.js", methods=["GET", "HEAD"])
  def render(request: Request):
    return FileResponse(args['render'])

  @app.route("/header", methods=["GET", "HEAD"])
  def header(request: Request):
    return JSONResponse(content=column_names(args))

  @app.route("/data/", methods=["GET", "HEAD"])
  def data(request: Request):

    query_params = dict(request.query_params)

    if not "_start" in query_params:
      # No server-side processing. Serve entire file or table.
      data, ntotal, nfiltered = query(args)
      return JSONResponse(content={"data": data})

    data, ntotal, nfiltered = query(args, query_params=query_params)

    content = {
                "draw": query_params["_draw"],
                "recordsTotal": ntotal,
                "recordsFiltered": nfiltered,
                "data": data
              }

    return JSONResponse(content=content)


if __name__ == "__main__":
  serve(cli())
