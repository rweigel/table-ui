import utilrsw
import tableui

config = {
  "use_all_attributes": True,
  "path_type": "dict",
  "paths": {
    "/": {
      "server": None,
      "id": None
    }
  }
}
datasets = {
  'server1':
    [
      {
        'id': 'ds1 id',
        'title': 'ds1 title',
        'info': {
          'start': 'ds1 start',
          'parameters': [
            {'name': 'param1', 'value': 'value1'},
          ]
        }
      },
      {
        'id': 'ds2 id',
        'title': 'ds2 title',
        'info': {
          'start': 'ds2 start',
          'parameters': [
            {'name': 'param1', 'value': 'value2'},
          ]
        }
      }
    ],
    'server2': [
      {
        'id': 'ds1 id',
        'title': 'ds1 title',
        'info': {
          'start': 'ds1 start',
          'parameters': [
            {'name': 'param1', 'value': 'value1'}
          ]
        }
      },
      {
        'id': 'ds2 id',
        'title': 'ds2 title',
        'info': {
          'start': 'ds2 start',
          'parameters': [
            {'name': 'param1', 'value': 'value2'}
          ]
        }
      }
    ]
  }
datasets_f = {}
for server_key in datasets.keys():
  for dataset in datasets[server_key]:
    # Put server key first
    dataset = {'server': server_key, **dataset}
    if 'info' in dataset:
      for k, v in dataset['info'].items():
        if k != 'parameters':
          dataset[k] = v
      del dataset['info']
    # Insert 'server' as the first key in the dataset dictionary
    datasets_f[f"{server_key}:{dataset['id']}"] = dataset


utilrsw.print_dict(datasets_f)
info = tableui.dict2sql(datasets_f, config, 'dict2sql_demo')
utilrsw.print_dict(info)
