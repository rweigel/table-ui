import os
import sys
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
  if len(sys.argv) > 1:
    file_head = sys.argv[2]
  if len(sys.argv) > 2:
    file_body = sys.argv[3]

  return {
          'port': int(port),
          'host': host,
          'file_head': file_head,
          'file_body': file_body,
          'file_conf': file_conf
        }

args = cli()
root_dir = os.path.dirname(__file__)

app = FastAPI()

def querydb(conn, name, query="", orders=None, offset=1, limit=18446744073709551615):

  # 18446744073709551615 is the maximum value for a 64-bit unsigned integer
  # See https://stackoverflow.com/a/271650 for why used.

  cursor = conn.cursor()

  orderby = ""
  if orders is not None:
    orderby = "ORDER BY "
    for order in orders:
      if order.startswith("-"):
        orderby += f"{order[1:]} DESC, "
      else:
        orderby += f"{order} ASC, "
    orderby = orderby[:-2]

  subset = ""
  if offset != 1:
    subset = f'LIMIT {limit} OFFSET {offset}'

  query = f"SELECT * FROM `{name}` {query} {orderby} {subset}"
  print(query)
  cursor.execute(query)
  return cursor.fetchall()

def query(start=None, end=None):
  if start is None:
    return DATA, len(DATA), len(DATA)

  return DATA[start:end], len(DATA), len(DATA)

def ids2colnums(cids):
  scols = []
  for cid in cids:
    for hidx, hid in enumerate(HEAD):
      if hid == cid:
        scols.append(hidx)
  return scols

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
  return JSONResponse(content=HEAD)

@app.route("/data/", methods=["GET", "HEAD"])
def data(request: Request):

  parameters = dict(request.query_params)

  if not "start" in parameters:
    # No server-side processing. Serve entire file.
    data, recordsTotal, recordsFiltered = query()
    return JSONResponse(content={"data": data})

  start = int(parameters["start"])
  end = int(parameters["start"]) + int(parameters["length"])
  data, recordsTotal, recordsFiltered = query(start=start, end=end)

  content = {
              "draw": parameters["draw"],
              "recordsTotal": recordsTotal,
              "recordsFiltered": recordsFiltered,
              "data": data
            }

  return JSONResponse(content=content)

# Serve static files
app.mount("/", StaticFiles(directory=root_dir), name="root")

if not os.path.exists(args['file_head']):
  print("WARNING: File not found: " + args['file_head'])
  HEAD = {}
else:
  with open(args['file_head']) as f:
    HEAD = json.load(f)

if not os.path.exists(args['file_body']):
  print("ERROR: File not found: " + args['file_body'])
  exit(1)

if args['file_body'].endswith(".json"):
  with open(args['file_body']) as f:
    print("Reading: " + args['file_body'])
    DATA = json.load(f)
    DATA_mtime_last = os.path.getmtime(args['file_body'])
else:
  import sqlite3

  print("Connecting to database file " + args['file_body'])
  conn = sqlite3.connect(args['file_body'])

  print("Querying database.")
  if len(sys.argv) == 4:
    name = os.path.basename(args['file_body'].replace(".sql", ""))
  else:
    name = os.path.basename(sys.argv[4])

  DATA = querydb(conn, name)

if __name__ == "__main__":
  ukwargs = {
              'host': args['host'],
              'port': args['port'],
              'server_header': False
            }
  uvicorn.run(app, **ukwargs)