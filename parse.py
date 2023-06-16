import re

original_dict = {
    'columns[0][data]': '0',
    'columns[0][name]': 'First',
    'columns[0][searchable]': 'true',
    'columns[0][orderable]': 'true',
    'columns[0][search][value]': 'My filter',
    'columns[0][search][regex]': 'false',
    'columns[1][data]': '0',
    'columns[1][name]': 'First',
    'columns[1][searchable]': 'true',
    'columns[1][orderable]': 'true',
    'columns[1][search][value]': 'My filter',
    'columns[1][search][regex]': 'false'
}

converted_dict = {}

columns = {}
for key, value in original_dict.items():
    keys = key.split('[')
    keys = [key.replace(']', '') for key in keys]
    print(keys, value)
    keys[1] = int(keys[1])
    if not keys[1] in columns:
        columns[keys[1]] = {}
    columns[keys[1]][keys[2]] = value

final_dict = {}
for key, value in columns.items():
    name = columns[key]['name']
    final_dict[name] = value
    del final_dict[name]['name']

print(columns)
print(final_dict)