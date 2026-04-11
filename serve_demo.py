import utilrsw
import tableui

# Convert json to sqlite3 database
table_name = "demo"
body = 'tableui/ui/demo/demo.body.json'
head = 'tableui/ui/demo/demo.head.json'
kwargs = {
  'types': {'d': 'INTEGER'},
  'out': 'tableui/ui/demo/demo.sqlite'
}
tableui.lists2table(table_name, body, head, **kwargs)

# Run server
configs = tableui.cli()
configs['app']['config'] = 'tableui/ui/conf/demo-sqlite.json'
utilrsw.uvicorn.run("tableui.app", configs)
