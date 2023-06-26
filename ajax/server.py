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

if not os.path.exists(args['file_body']):
  print("ERROR: File not found: " + args['file_body'])
  exit(1)
with open(args['file_body']) as f:
  DATA = json.load(f)
DATA_mtime_last = os.path.getmtime(args['file_body'])

if not os.path.exists(args['file_head']):
  print("WARNING: File not found: " + args['file_head'])
  HEAD = {}
else:
  with open(args['file_head']) as f:
    HEAD = json.load(f)

def cors_headers(response: Response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, HEAD, OPTIONS"
    return response 

@app.route("/", methods=["GET", "HEAD"])
def config(request: Request):
  return FileResponse('index.html')

@app.route("/config", methods=["GET", "HEAD"])
def config(request: Request):
  if os.path.exists(args['file_conf']):
    return FileResponse(args['file_conf'])
  else:
    return JSONResponse(content={})

@app.route("/header", methods=["GET", "HEAD"])
def header(request: Request):
  return JSONResponse(content=HEAD)

@app.route("/data/", methods=["GET", "HEAD"])
def data(request: Request):

  parameters = dict(request.query_params)

  recordsTotal = len(DATA)

  if not "start" in parameters:
    # No server-side processing. Serve entire file.
    return JSONResponse(content={"data": DATA})

  start = int(parameters["start"])
  end = int(parameters["start"]) + int(parameters["length"])
  data = DATA[start:end]

  content = {
              "draw": parameters["draw"],
              "recordsTotal": recordsTotal,
              "recordsFiltered": recordsTotal,
              "data": data
            }

  return JSONResponse(content=content)

# Serve static files
app.mount("/", StaticFiles(directory=root_dir), name="root")

if __name__ == "__main__":
  ukwargs = {
              'host': args['host'],
              'port': args['port'],
              'server_header': False
            }
  uvicorn.run(app, **ukwargs)