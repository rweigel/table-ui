# Usage:
#   python server_test.py
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
  serve(port=kwargs['port'], config=kwargs['config'])


def _log_test_title(url):
  line = len(url)*"-"
  logger.info(line)
  logger.info(f"Testing {url}")
  logger.info(line)

def _wait_for_server(url, retries=50, delay=0.2):
  # Wait for the server to start
  print("Checking if server is ready by making request to /config ...")
  for i in range(retries):
    try:
      response = requests.get(url, timeout=0.5)
      if response.status_code == 200:
        break
    except Exception:
      print(f"Server not ready. Next try in {delay} sec...")
      time.sleep(delay)
  else:
    raise RuntimeError(f"Server did not start after {retries} attempts.")

def _run_tests(port, config, head_data, body_data):

  server_kwargs = {
    "port": port,
    "config": config,
  }

  proc_kwargs = {
    "target": _server_test_run_server,
    "args": (server_kwargs,),
    "daemon": True
  }

  server_process = multiprocessing.Process(**proc_kwargs)
  server_process.start()

  base = f"http://127.0.0.1:{port}"
  url = f"{base}/config"

  _wait_for_server(url, retries=50, delay=0.5)

  _log_test_title(url)
  response = requests.get(url)
  assert response.status_code == 200
  assert 'dataTables' in response.json()
  assert 'dataTablesAdditions' in response.json()

  url = f"{base}/data/"
  _log_test_title(url)
  response = requests.get(url)
  assert response.status_code == 200
  assert 'data' in response.json()
  assert response.json()['data'] == body_data

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
  assert list(response.json()['data'][0].keys()) == head_data

  if 'sqldb' in config:
    url = f"{base}/data/?_start=0&_length=2&_draw=10"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['draw'] == 10
    assert response.json()['recordsTotal'] == len(body_data)
    assert response.json()['recordsFiltered'] == len(body_data)
    assert len(response.json()['data']) == 2
    assert len(response.json()['data'][0]) == len(head_data)
    for i in range(2):
      for j in range(len(head_data)):
        assert response.json()['data'][i][j] == body_data[i][j]

    url = f"{base}/data/?_orders=-{head_data[0]}"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(body_data)
    assert response.json()['recordsFiltered'] == len(body_data)
    assert len(response.json()['data']) == len(body_data)
    print(response.json()['data'])
    for i in range(len(body_data)):
      for j in range(len(head_data)):
        assert response.json()['data'][i][j] == body_data[len(body_data)-1-i][j]

    _start = 2
    _length = 2
    url = f"{base}/data/?_start={_start}&_length={_length}"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(body_data)
    assert response.json()['recordsFiltered'] == len(body_data)
    assert len(response.json()['data']) == 2
    assert len(response.json()['data'][0]) == len(head_data)
    for i in range(_length):
      for j in range(len(head_data)):
        assert response.json()['data'][i][j] == body_data[i+_start][j]

    cols = f"{head_data[0]},{head_data[2]}"
    url = f"{base}/data/?_return={cols}"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(body_data)
    assert response.json()['recordsFiltered'] == len(body_data)
    assert len(response.json()['data']) == len(body_data)
    assert len(response.json()['data'][0]) == 2
    response.json()['data'][0][0] == body_data[0][0]
    response.json()['data'][0][1] == body_data[0][2]

    logger.info(40*"-")
    cols = f"{head_data[0]},{head_data[2]}"
    url = f"{base}/data/?_verbose=true&_return={cols}"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(body_data)
    assert response.json()['recordsFiltered'] == len(body_data)
    assert len(response.json()['data']) == len(body_data)
    assert len(response.json()['data'][0]) == 2
    response.json()['data'][0]['a'] == body_data[0][0]
    response.json()['data'][0]['c'] == body_data[0][2]

    url = f"{base}/data/?a=a01"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(body_data)
    assert response.json()['recordsFiltered'] == 1
    assert len(response.json()['data']) == 1
    assert len(response.json()['data'][0]) == len(head_data)
    for i in range(len(head_data)):
      assert response.json()['data'][0][i] == body_data[0][i]

    url = f"{base}/data/?a=a_1"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(body_data)
    assert response.json()['recordsFiltered'] == 2
    assert len(response.json()['data']) == 2
    assert len(response.json()['data'][0]) == len(head_data)
    assert response.json()['data'][0][0] == 'a01'
    assert response.json()['data'][1][0] == 'a11'

    url = f"{base}/data/?_verbose=true&a=a01"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(body_data)
    assert response.json()['recordsFiltered'] == 1
    assert len(response.json()['data']) == 1
    assert len(response.json()['data'][0]) == len(head_data)
    for i, col in enumerate(head_data):
      assert response.json()['data'][0][col] == body_data[0][i]

    url = f"{base}/data/?a=a01&_return={cols}"
    cols = f"{head_data[0]},{head_data[2]}"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(body_data)
    assert response.json()['recordsFiltered'] == 1
    assert len(response.json()['data']) == 1
    assert len(response.json()['data'][0]) == 2
    response.json()['data'][0][0] == body_data[0][0]
    response.json()['data'][0][1] == body_data[0][2]

    cols = f"{head_data[0]},{head_data[2]}"
    url = f"{base}/data/?_verbose=true&a=a01&_return={cols}"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    assert 'data' in response.json()
    assert response.json()['recordsTotal'] == len(body_data)
    assert response.json()['recordsFiltered'] == 1
    assert len(response.json()['data']) == 1
    assert len(response.json()['data'][0]) == 2
    response.json()['data'][0]['a'] == body_data[0][0]
    response.json()['data'][0]['c'] == body_data[0][2]


    def check_uniques(data, head_data, body_data, cols=None):
      if cols is None:
        cols = head_data
      for i, col in enumerate(cols):
        assert col in data
        uniques = set()
        for val, count in data[col]:
          uniques.add(val)
        assert len(uniques) <= len(body_data)
        for row in body_data:
          assert row[head_data.index(col)] in uniques

    url = f"{base}/data/?_uniques=true"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert len(data.keys()) == len(head_data)
    check_uniques(data, head_data, body_data)

    cols = f"{head_data[0]},{head_data[2]}"
    url = f"{base}/data/?_uniques=true&_return={cols}"
    _log_test_title(url)
    response = requests.get(url)
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert list(data.keys()) == cols.split(",")
    check_uniques(data, head_data, body_data, cols=cols.split(","))

    cols = f"{head_data[0]},{head_data[2]}"
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

  scriptdir = os.path.dirname(__file__)
  root_dir = os.path.normpath(os.path.join(scriptdir, "..", "demo"))

  head_file = os.path.join(root_dir, 'demo.head.json')
  body_file = os.path.join(root_dir, 'demo.body.json')

  with open(head_file) as f:
    head_data = json.load(f)
  with open(body_file) as f:
    body_data = json.load(f)

  # Test 1
  config = {"jsondb": {"head": head_file, "body": body_file}}

  _run_tests(port, config, head_data, body_data)

  # Test 2
  table_name = "demo"
  config = {"table_name": table_name}
  import tableui
  # Convert json to sqlite3 database
  sqldb_path = tableui.list2sql(table_name, body_file, json_head=head_file)
  config["sqldb"] = sqldb_path

  _run_tests(port, config, head_data, body_data)
