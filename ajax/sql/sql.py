import os
import sys
import time
import sqlite3

def read_and_prep(header_file, matrix_file):

  def unique(header):

    headerlc = [val.lower() for val in header]
    headeru = header.copy()
    for val in header:
      indices = [i for i, x in enumerate(headerlc) if x == val.lower()]
      if len(indices) > 1:
        dups = [header[i] for i in indices]
        print("ERROR: Duplicate column names when cast to lower case: " + str(dups))
        for r, idx in enumerate(indices):
          if r > 0:
            newname = header[idx] + "_$" + str(r) + "$"
            #print("Renaming " + header[idx] + " to " + newname)
            headeru[idx] = newname
    return headeru

  #import json
  #print(json.dumps(header,indent=2))

  import json
  with open(header_file) as f:
    print("Reading " + header_file)
    header = json.load(f)
  with open(matrix_file) as f:
    print("Reading " + matrix_file)
    matrix = json.load(f)

  print("Casting table elements to str.")
  start = time.time()
  header = unique(header)
  for i, row in enumerate(matrix):
    for j, col in enumerate(row):
      if len(matrix[i]) != len(header):
        print("ERROR: Number of columns in row " + str(i) + " does not match header.")
        exit()
      matrix[i][j] = str(matrix[i][j])

  dt = "{:.2f} [s]".format(time.time() - start)
  print(f"Took {dt} to cast elements as strings in table with {len(matrix)} rows and {len(header)} columns.")
  return header, matrix

def createdb(table, file="table1.db", name="table1", header=None):

  for hidx, colname in enumerate(header):
    header[hidx] = f"`{colname}`"

  column_names = f"({', '.join(header)})"
  column_spec  = f"({', '.join(header)} TEXT)"
  column_vals  = f"({', '.join(len(header)*['?'])})"

  create  = f'CREATE TABLE `{name}` {column_spec}'
  execute = f'INSERT INTO `{name}` {column_names} VALUES {column_vals}'

  conn = sqlite3.connect(file)
  cursor = conn.cursor()
  print(create)
  cursor.execute(create)

  print(execute)
  cursor.executemany(execute, table)
  conn.commit()

  if header is not None:
    index = f"CREATE INDEX idx0 ON `{name}` ({header[0]})"
    print(index)
    cursor.execute(index)
    conn.commit()

  conn.close()

def test():
  conn = sqlite3.connect(file)
  cursor = conn.cursor()

  start = time.time()
  query = f"SELECT * FROM `{name}` ORDER BY {header[0]} ASC"
  print(query)
  cursor.execute(query)
  resp = cursor.fetchall()
  dt = "{:.2f} [s]".format(time.time() - start)
  print(f"Took {dt} to sort all records by {header[0]}")

file = sys.argv[2] + ".sql"
if os.path.exists(file):
  os.remove(file)

name = os.path.basename(sys.argv[2])
header, matrix = read_and_prep(sys.argv[1], sys.argv[2])
print("Creating database file " + file)
start = time.time()
createdb(matrix, header=header, file=file, name=name)
dt = "{:.2f} [s]".format(time.time() - start)
print(f"Took {dt} to create database")

test()