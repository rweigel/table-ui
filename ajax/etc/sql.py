import os
import sys
import sqlite3

def read_and_prep(header_file, matrix_file):

  def unique(header):

    headerlc = [val.lower() for val in header]
    headeru = header.copy()
    for val in header:
      indices = [i for i, x in enumerate(headerlc) if x == val.lower()]
      if len(indices) > 1:
        dups = [header[i] for i in indices]
        print("ERROR: Duplicate column names: " + str(dups))
        for r, idx in enumerate(indices):
          if r > 0:
            newname = header[idx] + "_$" + str(r) + "$"
            print("Renaming " + header[idx] + " to " + newname)
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
  import time
  start = time.time()
  header = unique(header)
  for i, row in enumerate(matrix):
    for j, col in enumerate(row):
      if len(matrix[i]) != len(header):
        print("ERROR: Number of columns in row " + str(i) + " does not match header.")
        exit()
      matrix[i][j] = str(matrix[i][j])

  print(f"Table has {len(matrix)} rows and {len(header)} columns.")
  end = time.time()
  print(end - start)
  return header, matrix

def createdb(table, file="table.db", name="table", header=None):

  column_names = f"({', '.join(header)})"
  column_spec  = f"({', '.join(header)} TEXT)"
  column_vals  = f"({', '.join(len(header)*['?'])})"

  create  = f'CREATE TABLE {name} {column_spec}'
  execute = f'INSERT INTO {name} {column_names} VALUES {column_vals}'

  #print(create)
  #print(execute)

  conn = sqlite3.connect(file)
  #conn = sqlite3.connect(":memory:")
  cursor = conn.cursor()
  cursor.execute(create)
  cursor.execute("CREATE INDEX idx_datasetID ON matrix (datasetID)")
  cursor.executemany(execute, table)
  conn.commit()
  #print(list(cursor.execute('SELECT * FROM matrix')))
  #conn.close()
  #return conn

def querydb(conn, name, orders=None, offset=1, limit=18446744073709551615):

  # 18446744073709551615 is the maximum value for a 64-bit unsigned integer
  # See https://stackoverflow.com/a/271650 for why used.

  cursor = conn.cursor()

  orderby = ""
  if orders is not None:
    orderby = "ORDER BY "
    for order in orders:
      if order.startswith("-"):
        orderby += f"{order[1:]} DESC, "
      else:
        orderby += f"{order} ASC, "
    orderby = orderby[:-2]

  subset = ""
  if offset != 1:
    subset = f'LIMIT {limit} OFFSET {offset}'

  query = f"SELECT * FROM {name} WHERE datasetID LIKE 'AC_H0_MFI' {orderby} {subset}"
  print(query)
  cursor.execute(query)
  return cursor.fetchall()

regen = True
name = "matrix"
file = "matrix.db"
#file = ":memory:"

if regen or not os.path.exists(file):
  if regen and os.path.exists(file):
    os.remove(file)
  #header = ["id", "version"]
  #matrix = [["A", "A"], ["A", "Ax"], ["B", "B"]]
  header, matrix = read_and_prep(sys.argv[1], sys.argv[2])
  print("Creating database file " + file)
  conn = createdb(matrix, header=header, file=file, name=name)

#orders = ["-"+header[0], header[1]]
print("Connecting to database file " + file)
conn = sqlite3.connect(file)
#print("Connecting to database file in memory.")
#conn = sqlite3.connect("file::memory:?cache=shared", uri=True)
#conn = sqlite3.connect(':memory:')
import time
start = time.time()
print("Querying database.")
#smatrix = querydb(conn, name, orders=orders)
smatrix = querydb(conn, name, offset=1, limit=10)
print(len(smatrix))
end = time.time()
print(end - start)
print("Done.")

#for row in smatrix: print(row)
print("Closing connection.")
conn.close()
print("Done.")