import utilrsw
import tableui

# Convert json to sqlite3 database
table_name = "demo"
body = 'demo/demo.body.json'
head = 'demo/demo.head.json'
kwargs = {
  'types': {'d': 'INTEGER'},
  'out': 'demo/demo.sqlite'
}
tableui.list2sql(table_name, body, head, **kwargs)

# Run server
configs = tableui.cli()
utilrsw.uvicorn.run("tableui.app", configs)
