import json
import copy

import utilrsw
import tableui

logger = utilrsw.logger('dicts2table_demo', console_format='%(message)s', log_level='DEBUG')

def print_summary(config, data, title):

  logger.info(utilrsw.hline(display=False))
  logger.info(title)
  logger.info(utilrsw.hline(display=False))

  info = tableui.dicts2table(data, config, embed=True)

  logger.info("config:")
  logger.info(json.dumps(config, indent=2))
  logger.info("")

  logger.info("dicts2table input:")
  logger.info(json.dumps(data, indent=2))
  logger.info("")

  logger.info("dicts2table output:")
  utilrsw.print_dict(info, indent=2)
  logger.info("")

  table_names = tableui.sql.table_names(info['sql'])
  logger.info(f"Table Names:\n  {table_names}\n")

  for table_name in table_names:
    tableui.sql.print_table(info['sql'], table_name)
    logger.info("")

  logger.info(utilrsw.hline(display=False))
  logger.info("")


config0 = {
  "use_all_attributes": True,
  "out_dir": 'dicts2table_demo',
  "name": "demo",
  "paths": {
    "/": {
      "id": "id",
      "attribute1": None,
      "attribute2": None,
      "attribute3": {"type": "integer"}
    }
  },
  "column_definitions": {
    "attribute1": "Attribute 1 definition",
    "attribute3": "Attribute 3 definition"
  }
}

data1 = [
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

title = "Reference config and data."
print_summary(config0, data1, title)


title = "use_all_attributes=False, only id and attribute1 in path '/'."
config = copy.deepcopy(config0)
config['use_all_attributes'] = False
del config['paths']['/']['attribute2']
del config['paths']['/']['attribute3']
print_summary(config, data1, title)


title = "use_all_attributes=True and omit_attributes=['attribute2']."
config = copy.deepcopy(config0)
config['omit_attributes'] = ['attribute2']
print_summary(config, data1, title)


title = "Example of using fix_attributes to fix misspelled attribute names."
config = copy.deepcopy(config0)
config['fix_attributes'] = True
config['fixes'] = {
  'attribute2typo': 'attribute2',
  'attribute3typo': 'attribute3'
}
data1[0]['attribute2typo'] = data1[0]['attribute2']
data1[0]['attribute3typo'] = data1[0]['attribute3']
del data1[0]['attribute2']
del data1[0]['attribute3']
print_summary(config, data1, title)


title = "Example using formats"
config = copy.deepcopy(config0)
config['formats'] = ['json'] # Allowed: 'json', 'json-split', 'csv', 'sql'
print_summary(config, data1, title)
