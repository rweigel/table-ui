DATA = [
  ["A","Ax"],
  ["B","B"],
  ["B","A"]
]

HEAD = [
  "id",
  "version"
]

sids = ['version']

if True:
  import sys
  import json
  with open(sys.argv[2]) as f:
    DATA = json.load(f)
  with open(sys.argv[1]) as f:
    HEAD = json.load(f)

class reverser:
  # https://stackoverflow.com/a/56842689
  def __init__(self, obj):
    self.obj = obj

  def __eq__(self, other):
    return other.obj == self.obj

  def __lt__(self, other):
    return other.obj < self.obj

def process(sids, DATA, HEAD):

  def ids2colnums(cids, HEAD):
    scols = []
    for cid in cids:
      for hidx, hid in enumerate(HEAD):
        if hid == cid:
          scols.append(hidx)
    return scols

  def parse(sids):
    # zip(*[(cid[1:],-1) if cid[0] == '-' else (cid,1) for cid in sids])
    sdirs = [None] * len(sids)
    for i, sid in enumerate(sids):
      if sid[0] == '-':
        sids[i] = sid[1:]
        sdirs[i] = -1
      else:
        sdirs[i] = 1
    return sids, sdirs

  def sort(DATA, cols, dirs):

    # Sort in place and treat all elements as strings
    def sort_key(row):
      slist = []
      # snum is the index of the list of columns to sort by
      # cnum is the index of the column in the row
      for snum, cnum in enumerate(cols):
        if dirs[snum] == 1:
          slist.append(str(row[cnum]))
        else:
          slist.append(reverser(str(row[cnum])))
          #slist.append(-ord(str(row[cnum])))

      return slist

    DATA.sort(key=sort_key)

  names, dirs = parse(sids)
  cols = ids2colnums(names,HEAD)
  sort(DATA, cols, dirs)

process(sids, DATA, HEAD)

for row in DATA:
  print(row)