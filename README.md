# Usage Examples

Serve table from JSON (all processing client side) at http://0.0.0.0:5002/

```
python serve.py \
  --port 5002 \
  --json-head demo/demo.head.json \
  --json-body demo/demo.body.json \
```

Serve table from SQLite (all processing server side) at http://0.0.0.0:5002/

```
python json2sql.py \
  --json-body demo/demo.body.json
  --json-head demo/demo.head.json
  --out demo/demo.body.sqlite
python serve.py \
  --port 5002 \
  --sqldb demo/demo.body.sqlite
```

# Testing

```
python -m tableui.serve_test
```