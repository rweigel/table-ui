# Usage Examples

## JSON data

All search and query processing client side

Serve demo/demo.body.json
```
tableui-serve --config tableui/uiconf/demo.json --port 5001
```

Serve multiple tables with config `tableui/uiconf/demos.json` and data referenced therein
```
tableui-serve --config tableui/uiconf/demos.json --port 5002
```

## SQLite database

For a SQLite database, processing for search and query is server-side.

Create and serve SQLite database.

```python
tableui-serve --config tableui/uiconf/demo-sqlite.json
```

# Test

```
python -m tableui.serve_test
```