import tableui

table_name = "list2sql_demo"
body = [["a01", "b01", 1], ["a02", "b02", 2]]
head = ["a", "b", "c"]
types = ["TEXT", "TEXT", "INTEGER"]
sqldb_path = tableui.list2sql(table_name, body, head, types=types)

import sqlite3
conn = sqlite3.connect(sqldb_path)
cursor = conn.cursor()
cursor.execute(f"SELECT * FROM `{table_name}` WHERE c = 1")
rows = cursor.fetchall()
conn.close()

assert rows == [("a01", "b01", 1)], f"Unexpected rows: {rows}"
print(f"PASS: rows where c = 1: {rows}")
