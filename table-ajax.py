import os
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware

port = 8001
host = "0.0.0.0"
root_dir = os.path.dirname(__file__)

app = FastAPI()

def parse_qs(original_dict):

    columns = {}
    for key, value in original_dict.items():
        if not key.startswith('columns'):
            continue;

        keys = key.split('[')
        keys = [key.replace(']', '') for key in keys]
        print(keys, value)
        keys[1] = int(keys[1])
        if not keys[1] in columns:
            columns[keys[1]] = {}
        columns[keys[1]][keys[2]] = value

    final_dict = {}
    for key, value in columns.items():
        name = columns[key]['name']
        final_dict[name] = value
        del final_dict[name]['name']
    return final_dict

def cors_headers(response: Response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, HEAD, OPTIONS"
    return response 

@app.route("/data/", methods=["GET", "HEAD"])
def data(request: Request):

    headers = dict(request.headers)
    parameters = dict(request.query_params)
    print(parse_qs(parameters))

    content = {
                    "draw": parameters["draw"],
                    "recordsTotal": 1,
                    "recordsFiltered": 1,
                    "data": [[
                        "Airi",
                        "Satou",
                        "Accountant",
                        "Tokyo",
                        "28th Nov 08",
                        "$162,700"
                        ]]}
    # Modify the response headers
    response = JSONResponse(content=content)

    return response

# Mount the static files directory
app.mount("/", StaticFiles(directory=root_dir), name="root")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=host, port=port, server_header=False)