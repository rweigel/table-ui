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
python serve.py --config conf/demo-sqlite.json
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