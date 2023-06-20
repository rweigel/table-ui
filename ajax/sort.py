# Step 2: Define the custom sorting function
def sorting_key(row, idx):
  return row[idx]

matrix = [["A","B"],["A","A"],["B","B"]]
# Step 3: Sort the matrix using the custom sorting function

idx = 0
key=lambda row: row
sorted_matrix = sorted(matrix, key=key, reverse=False)

# Step 4: Print the sorted matrix
for row in sorted_matrix:
  print(row)