import os
import json

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, FileResponse
from fastapi.staticfiles import StaticFiles

port = 8001
host = "0.0.0.0"
file_header = "cdaweb-hapi/data/tables/all.table.header.json"
file_body = "cdaweb-hapi/data/tables/all.table.body.json"
file_conf = "cdaweb-hapi/table/table.json"
root_dir = os.path.dirname(__file__)

app = FastAPI()

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
  if os.path.exists(file_conf):
    return FileResponse(file_conf)
  else:
    return JSONResponse(content={})

@app.route("/header", methods=["GET", "HEAD"])
def header(request: Request):
  if os.path.exists(file_header):
    return FileResponse(file_header)
  else:
    return JSONResponse(content={})

@app.route("/data/", methods=["GET", "HEAD"])
def data(request: Request):

  if not os.path.exists(file_header):
    return JSONResponse(content={})

  parameters = dict(request.query_params)

  with open(file_body) as f:
    data = json.load(f)

  recordsTotal = len(data)

  if "start" in parameters:
    start = int(parameters["start"])
    end = int(parameters["start"]) + int(parameters["length"])
    data = data[start:end]

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
  uvicorn.run(app, host=host, port=port, server_header=False)