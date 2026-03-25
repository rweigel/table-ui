import utilrsw
import tableui

config1 = {
  "use_all_attributes": True,
  "out_dir": 'dict2sql_demo/demo1',
  "paths": {
    "/": {
      "id": None,
      "attribute1": None,
      "attribute2": None,
      "attribute3": {"type": "integer", "description": "An integer"}
    }
  }
}

dataset1 = [
  {
    "id": "id_11",
    "attribute1": "id_11/attribute1",
    "attribute2": "id_11/attribute2",
    "attribute3": 11
  },
  {
    "id": "id_12",
    "attribute1": "id_12/attribute1",
    "attribute2": "id_12/attribute2",
    "attribute3": 12
  },
  {
    "id": "id_21",
    "attribute1": "id_21/attribute1",
    "attribute2": "id_21/attribute2",
    "attribute3": 21
  },
  {
    "id": "id_22",
    "attribute1": "id_22/attribute1",
    "attribute2": "id_22/attribute2",
    "attribute3": 22
  }
]

info = tableui.dict2sql(dataset1, config1, embed=True)

print("Info:")
utilrsw.print_dict(info)

print("\nTable Names:")
table_names = tableui.sql.table_names(info['sql'])
print(table_names)

for table_name in table_names:
  print(f"\nTable '{table_name}' Contents:")
  tableui.sql.print(info['sql'], table_name)