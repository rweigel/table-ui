# Usage:
#   python -m tableui.serve_test
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

def _log_test_title(url):
  line = len(url)*"-"
  logger.info(line)
  logger.info(f"Testing {url}")
  logger.info(line)

def _server_test1(port, json_head, json_body, db_type):

  server_kwargs = {
    "port": port,
  }

  if db_type == 'json':
    server_kwargs["json_head"] = json_head
    server_kwargs["json_body"] = json_body
  if db_type == 'sql':
    import tableui
    table_name = "demo"
    sqldb_path = tableui.json2sql(table_name, json_body, json_head=json_head)
    server_kwargs["table_name"] = table_name
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

  url = f"{base}/config"
  _log_test_title(url)
  response = requests.get(url)
  assert response.status_code == 200
  assert 'tableUI' in response.json()

  url = f"{base}/data/"
  _log_test_title(url)
  response = requests.get(url)
  assert response.status_code == 200
  assert 'data' in response.json()
  assert response.json()['data'] == json_body_data

  url = f"{base}/data/?_invalid_key=true"
  _log_test_title(url)
  response = requests.get(url)
  assert response.status_code == 400
  print(response.json())
  assert 'error' in response.json()

  url = f"{base}/data/?_verbose=true"
  _log_test_title(url)
  response = requests.get(url)
  assert response.status_code == 200
  assert 'data' in response.json()
  assert list(response.json()['data'][0].keys()) == json_head_data

  if db_type == 'sql':
    url = f"{base}/data/?_start=0&_length=2&_draw=10"
    _log_test_title(url)
    response = requests.get(url)
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

    url = f"{base}/data/?_orders=-{json_head_data[0]}"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(json_body_data)
    assert response.json()['recordsFiltered'] == len(json_body_data)
    assert len(response.json()['data']) == len(json_body_data)
    print(response.json()['data'])
    for i in range(len(json_body_data)):
      for j in range(len(json_head_data)):
        assert response.json()['data'][i][j] == json_body_data[len(json_body_data)-1-i][j]

    _start = 2
    _length = 2
    url = f"{base}/data/?_start={_start}&_length={_length}"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(json_body_data)
    assert response.json()['recordsFiltered'] == len(json_body_data)
    assert len(response.json()['data']) == 2
    assert len(response.json()['data'][0]) == len(json_head_data)
    for i in range(_length):
      for j in range(len(json_head_data)):
        assert response.json()['data'][i][j] == json_body_data[i+_start][j]

    cols = f"{json_head_data[0]},{json_head_data[2]}"
    url = f"{base}/data/?_return={cols}"
    _log_test_title(url)
    response = requests.get(url)
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
    url = f"{base}/data/?_verbose=true&_return={cols}"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(json_body_data)
    assert response.json()['recordsFiltered'] == len(json_body_data)
    assert len(response.json()['data']) == len(json_body_data)
    assert len(response.json()['data'][0]) == 2
    response.json()['data'][0]['a'] == json_body_data[0][0]
    response.json()['data'][0]['c'] == json_body_data[0][2]

    url = f"{base}/data/?a=a01"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(json_body_data)
    assert response.json()['recordsFiltered'] == 1
    assert len(response.json()['data']) == 1
    assert len(response.json()['data'][0]) == len(json_head_data)
    for i in range(len(json_head_data)):
      assert response.json()['data'][0][i] == json_body_data[0][i]

    url = f"{base}/data/?a=a_1"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(json_body_data)
    assert response.json()['recordsFiltered'] == 2
    assert len(response.json()['data']) == 2
    assert len(response.json()['data'][0]) == len(json_head_data)
    assert response.json()['data'][0][0] == 'a01'
    assert response.json()['data'][1][0] == 'a11'

    url = f"{base}/data/?_verbose=true&a=a01"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(json_body_data)
    assert response.json()['recordsFiltered'] == 1
    assert len(response.json()['data']) == 1
    assert len(response.json()['data'][0]) == len(json_head_data)
    for i, col in enumerate(json_head_data):
      assert response.json()['data'][0][col] == json_body_data[0][i]

    url = f"{base}/data/?a=a01&_return={cols}"
    cols = f"{json_head_data[0]},{json_head_data[2]}"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(json_body_data)
    assert response.json()['recordsFiltered'] == 1
    assert len(response.json()['data']) == 1
    assert len(response.json()['data'][0]) == 2
    response.json()['data'][0][0] == json_body_data[0][0]
    response.json()['data'][0][1] == json_body_data[0][2]

    cols = f"{json_head_data[0]},{json_head_data[2]}"
    url = f"{base}/data/?_verbose=true&a=a01&_return={cols}"
    _log_test_title(url)
    response = requests.get(url)
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

    url = f"{base}/data/?_uniques=true"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert len(data.keys()) == len(json_head_data)
    check_uniques(data, json_head_data, json_body_data)

    cols = f"{json_head_data[0]},{json_head_data[2]}"
    url = f"{base}/data/?_uniques=true&_return={cols}"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert list(data.keys()) == cols.split(",")
    check_uniques(data, json_head_data, json_body_data, cols=cols.split(","))

    cols = f"{json_head_data[0]},{json_head_data[2]}"
    url = f"{base}/data/?_uniques=true&_return={cols}&_length=3"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert list(data.keys()) == cols.split(",")
    for col in data.keys():
      assert len(data[col]) <= 3

  server_process.terminate()
  server_process.join()


if __name__ == "__main__":
  port = 4977
  root_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "demo"))
  json_head = os.path.join(root_dir, 'demo.head.json')
  json_body = os.path.join(root_dir, 'demo.body.json')

  _server_test1(port, json_head, json_body, 'json')
  _server_test1(port, json_head, json_body, 'sql')
