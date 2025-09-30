# Usage:
#    python -m tableui.serve_test
import os
import time
import json
import logging
import requests
import multiprocessing

from tableui import serve

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


def _server_test_run_server(kwargs):
  serve(**kwargs)

def _server_test1(port, json_head, json_body, db_type):

  server_kwargs = {
    "port": port,
  }

  if db_type == 'json':
    server_kwargs["json_head"] =  json_head
    server_kwargs["json_body"] =  json_body
  if db_type == 'sql':
    import tableui
    sqldb_path = tableui.json2sql(json_head, json_body)
    server_kwargs["sqldb"] = sqldb_path

  proc_kwargs = {
    "target": _server_test_run_server,
    "args": (server_kwargs,),
    "daemon": True
  }

  server_process = multiprocessing.Process(**proc_kwargs)
  server_process.start()

  # Wait for the server to start
  time.sleep(0.5)

  with open(json_head) as f:
    json_head_data = json.load(f)
  with open(json_body) as f:
    json_body_data = json.load(f)

  base = f"http://127.0.0.1:{port}"

  logger.info(40*"-")
  response = requests.get(f"{base}/config")
  assert response.status_code == 200
  assert 'tableUI' in response.json()

  logger.info(40*"-")
  response = requests.get(f"{base}/data/")
  assert response.status_code == 200
  assert 'data' in response.json()
  assert response.json()['data'] == json_body_data

  logger.info(40*"-")
  response = requests.get(f"{base}/data/?_verbose=true")
  assert response.status_code == 200
  assert 'data' in response.json()
  assert list(response.json()['data'][0].keys()) == json_head_data

  if db_type == 'sql':
    logger.info(40*"-")
    response = requests.get(f"{base}/data/?_start=0&_length=2&_draw=10")
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['draw'] == 10
    assert response.json()['recordsTotal'] == len(json_body_data)
    assert response.json()['recordsFiltered'] == len(json_body_data)
    assert len(response.json()['data']) == 2
    assert len(response.json()['data'][0]) == len(json_head_data)
    for i in range(2):
      for j in range(len(json_head_data)):
        assert response.json()['data'][i][j] == json_body_data[i][j]

    logger.info(40*"-")
    response = requests.get(f"{base}/data/?_orders=-{json_head_data[0]}")
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(json_body_data)
    assert response.json()['recordsFiltered'] == len(json_body_data)
    assert len(response.json()['data']) == len(json_body_data)
    print(response.json()['data'])
    for i in range(len(json_body_data)):
      for j in range(len(json_head_data)):
        assert response.json()['data'][i][j] == json_body_data[len(json_body_data)-1-i][j]

    logger.info(40*"-")
    _start = 2
    _length = 2
    response = requests.get(f"{base}/data/?_start={_start}&_length={_length}")
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(json_body_data)
    assert response.json()['recordsFiltered'] == len(json_body_data)
    assert len(response.json()['data']) == 2
    assert len(response.json()['data'][0]) == len(json_head_data)
    for i in range(_length):
      for j in range(len(json_head_data)):
        assert response.json()['data'][i][j] == json_body_data[i+_start][j]

    logger.info(40*"-")
    cols = f"{json_head_data[0]},{json_head_data[2]}"
    response = requests.get(f"{base}/data/?_return={cols}")
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(json_body_data)
    assert response.json()['recordsFiltered'] == len(json_body_data)
    assert len(response.json()['data']) == len(json_body_data)
    assert len(response.json()['data'][0]) == 2
    response.json()['data'][0][0] == json_body_data[0][0]
    response.json()['data'][0][1] == json_body_data[0][2]

    logger.info(40*"-")
    cols = f"{json_head_data[0]},{json_head_data[2]}"
    response = requests.get(f"{base}/data/?_verbose=true&_return={cols}")
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(json_body_data)
    assert response.json()['recordsFiltered'] == len(json_body_data)
    assert len(response.json()['data']) == len(json_body_data)
    assert len(response.json()['data'][0]) == 2
    response.json()['data'][0]['a'] == json_body_data[0][0]
    response.json()['data'][0]['c'] == json_body_data[0][2]

    logger.info(40*"-")
    response = requests.get(f"{base}/data/?a=a01")
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(json_body_data)
    assert response.json()['recordsFiltered'] == 1
    assert len(response.json()['data']) == 1
    assert len(response.json()['data'][0]) == len(json_head_data)
    for i in range(len(json_head_data)):
      assert response.json()['data'][0][i] == json_body_data[0][i]

    logger.info(40*"-")
    response = requests.get(f"{base}/data/?_verbose=true&a=a01")
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(json_body_data)
    assert response.json()['recordsFiltered'] == 1
    assert len(response.json()['data']) == 1
    assert len(response.json()['data'][0]) == len(json_head_data)
    for i, col in enumerate(json_head_data):
      assert response.json()['data'][0][col] == json_body_data[0][i]

    logger.info(40*"-")
    cols = f"{json_head_data[0]},{json_head_data[2]}"
    response = requests.get(f"{base}/data/?a=a01&_return={cols}")
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(json_body_data)
    assert response.json()['recordsFiltered'] == 1
    assert len(response.json()['data']) == 1
    assert len(response.json()['data'][0]) == 2
    response.json()['data'][0][0] == json_body_data[0][0]
    response.json()['data'][0][1] == json_body_data[0][2]

    logger.info(40*"-")
    cols = f"{json_head_data[0]},{json_head_data[2]}"
    response = requests.get(f"{base}/data/?_verbose=true&a=a01&_return={cols}")
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(json_body_data)
    assert response.json()['recordsFiltered'] == 1
    assert len(response.json()['data']) == 1
    assert len(response.json()['data'][0]) == 2
    response.json()['data'][0]['a'] == json_body_data[0][0]
    response.json()['data'][0]['c'] == json_body_data[0][2]


    def check_uniques(data, json_head_data, json_body_data, cols=None):
      if cols is None:
        cols = json_head_data
      for i, col in enumerate(cols):
        assert col in data
        uniques = set()
        for val, count in data[col]:
          uniques.add(val)
        assert len(uniques) <= len(json_body_data)
        for row in json_body_data:
          assert row[json_head_data.index(col)] in uniques

    logger.info(40*"-")
    response = requests.get(f"{base}/data/?_uniques=true")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert len(data.keys()) == len(json_head_data)
    check_uniques(data, json_head_data, json_body_data)

    logger.info(40*"-")
    cols = f"{json_head_data[0]},{json_head_data[2]}"
    response = requests.get(f"{base}/data/?_uniques=true&_return={cols}")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert list(data.keys()) == cols.split(",")
    check_uniques(data, json_head_data, json_body_data, cols=cols.split(","))

  server_process.terminate()
  server_process.join()


if __name__ == "__main__":
  port = 4977
  root_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "demo"))
  json_head = os.path.join(root_dir, 'demo.head.json')
  json_body = os.path.join(root_dir, 'demo.body.json')

  _server_test1(port, json_head, json_body, 'json')
  _server_test1(port, json_head, json_body, 'sql')
