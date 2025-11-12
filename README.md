# Usage Examples

## JSON data

All search and query processing client side

Serve demo/demo.body.json
```
python serve.py --config conf/demo.json --port 5001
```

Serve multiple tables with config conf/demos.json and data referenced therein
```
python serve.py --config conf/demos.json --port 5002
```

## SQLite database

For a SQLite database, processing for search and query is server-side.

Create and serve SQLite database.

```python
import tableui
table_name = "demo"
# Convert json to sqlite3 database
json_body = 'demo/demo.body.json'
kwargs = {
  'json_head': 'demo/demo.head.json',
  'out': 'demo/demo.sqlite'
}
tableui.list2sql(table_name, json_body, **kwargs)
tableui.serve(config='conf/demo-sqlite.json')
# Or from shell
#  python serve.py --config conf/demo-sqlite.json
```

## More than one worker

To use more than one worker
```
CONFIG=conf/demos.json uvicorn tableui:factory --factory --workers 2 --port 5001
```

# Test

```
python -m tableui.serve_test
```