import os
import sys
import time
import json

try:
  import uvicorn
  import fastapi
except:
  print(os.popen('pip install uvicorn fastapi').read())

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, FileResponse
from fastapi.staticfiles import StaticFiles

def cli():

  port = sys.argv[1]
  host = "0.0.0.0"
  file_head = "data/hapi.table.head.json"
  file_body = "data/hapi.table.body.json"
  file_conf = "data/hapi.table.conf.json"

  if len(sys.argv) > 2:
    file_head = sys.argv[2]
  if not os.path.exists(file_head):
    print("WARNING: File not found: " + file_head)

  if len(sys.argv) > 3:
    file_body = sys.argv[3]
  if not os.path.exists(file_body):
    print("ERROR: File not found: " + file_body)
    exit(1)

  if len(sys.argv) > 4:
    table = os.path.basename(sys.argv[4])
  elif file_body.endswith(".sql"):
      table = os.path.basename(file_body.replace(".sql", ""))
  else:
    table = None
  if table is not None:
    # TODO: Check if table exists in database
    pass

  return {
          'port': int(port),
          'host': host,
          'file_head': file_head,
          'file_body': file_body,
          'file_conf': file_conf,
          'table': table,
          'root_dir': os.path.dirname(__file__)
        }


def column_names(args):
  if not os.path.exists(args['file_head']):
    #print("WARNING: File not found: " + args['file_head'])
    return {}
  with open(args['file_head']) as f:
    return json.load(f)


def querydb(conn, table, query="", orders=None, searches=None, offset=0, limit=None):

  cursor = conn.cursor()

  def execute(query):
    start = time.time()
    print(query)
    result = cursor.execute(query)
    data = result.fetchall()
    dt = "{:.4f} [s]".format(time.time() - start)
    print(f"Took {dt} to query and fetch")
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
    print("----")
    print(searches)
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

  if args['table'] is None:
    with open(args['file_body']) as f:
      # TODO: Stream
      print("Reading: " + args['file_body'])
      DATA = json.load(f)
      if start is None:
        return DATA, len(DATA), len(DATA)
      else:
        return DATA[start:end], len(DATA), len(DATA)
  else:
    import sqlite3

    orders = None
    if "_orders" in query_params:
      orders = query_params["_orders"].split(",")

    import urllib
    searches = {}
    for key, value in query_params.items():
      if key in column_names(args):
        #searches[key] = query_params[key]
        searches[key] = urllib.parse.unquote(query_params[key], encoding='utf-8', errors='replace')

    print("Connecting to database file " + args['file_body'])
    conn = sqlite3.connect(args['file_body'])
    DATA, ntotal, nfiltered = querydb(conn, args['table'], orders=orders, searches=searches, offset=start, limit=end-start)
    conn.close()
    return DATA, ntotal, nfiltered


def api_init(app, args):

  def cors_headers(response: Response):
      response.headers["Access-Control-Allow-Origin"] = "*"
      response.headers["Access-Control-Allow-Headers"] = "*"
      response.headers["Access-Control-Allow-Methods"] = "GET, HEAD, OPTIONS"
      return response 

  @app.route("/", methods=["GET", "HEAD"])
  def config(request: Request):
    return FileResponse(os.path.join(os.path.dirname(__file__),'index.html'))

  @app.route("/config", methods=["GET", "HEAD"])
  def config(request: Request):
    if args['file_body'].endswith(".sql"):
      return JSONResponse(content={"serverSide": True})
    else:
      return JSONResponse(content={})

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

  # Serve static files
  app.mount("/", StaticFiles(directory=args['root_dir']), name="root")


args = cli()
app = FastAPI()
api_init(app, args)

if __name__ == "__main__":
  ukwargs = {
              'host': args['host'],
              'port': args['port'],
              'server_header': False
            }
  uvicorn.run(app, **ukwargs)