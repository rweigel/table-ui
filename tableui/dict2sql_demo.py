import tableui

config1 = {
  "use_all_attributes": True,
  "paths": {
    "/": {
      "id": None,
      "attribute1": None,
      "attribute2": None
    }
  }
}
datasets1 = [
  {
    "id": "id_11",
    "attribute1": "id_11/attribute1",
    "attribute2": "id_11/attribute2"
  },
  {
    "id": "id_12",
    "attribute1": "id_12/attribute1",
    "attribute2": "id_12/attribute2"
  },
  {
    "id": "id_21",
    "attribute1": "id_21/attribute1",
    "attribute2": "id_21/attribute2"
  },
  {
    "id": "id_22",
    "attribute1": "id_22/attribute1",
    "attribute2": "id_22/attribute2"
  }
]
out_dir1 = 'dict2sql_demo/dict2sql_demo1'
info = tableui.dict2sql(datasets1, config1, out_dir1, embed=True)

