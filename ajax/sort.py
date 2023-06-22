matrix = [["A","B"],["A","A"],["B","B"]]

types = []

def sort_key(row):
  return row[0], -ord(row[1])  # Sort second column in descending order

matrix.sort(key=sort_key)

for row in matrix:
    print(row)