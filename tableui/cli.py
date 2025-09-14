import os
import tableui

def defaults(args):

  if not os.path.isabs(args['render']):
    args['render'] = os.path.join(args['root_dir'], args['render'])

  if not os.path.isabs(args['config']):
    args['config'] = os.path.join(args['root_dir'], args['config'])

  if not args['sqldb'] and not args['json_head'] and not args['json_body']:
    print("ERROR: Must specify --sqldb or --json-header and --json-body")
    exit(1)

  if args['sqldb'] is None:
    if not args['json_head'] or not args['json_body']:
      print("ERROR: Must specify --json-header and --json-body if not using --sqldb")
      exit(1)

    file_head = args['json_head']
    if file_head is None:
      print("WARNING: No json-head file given. Using indices as column names.")
    else:
      if not os.path.exists(file_head):
        args['json_head'] = None
        print("WARNING: File not found: " + file_head + ". Using indices as column names.")

    json_body = args['json_body']
    if not os.path.exists(json_body):
      print("ERROR: File not found: " + json_body)
      exit(1)

  return args

def args():

  def get_keywords(func):
    import inspect
    """Return a dict of (reversed) keyword arguments from a function."""
    spec = inspect.getfullargspec(func)
    keys = reversed(spec.args)
    values = reversed(spec.defaults)
    return {k: v for k, v in zip(keys, values)}

  kwargs = get_keywords(tableui.serve)

  return {
    "sqldb": {
      "help": "File containing SQL database",
      "default": kwargs['sqldb']
    },
    "table-name": {
      "help": "Name of table in sqldb. Defaults to SQLDB without extension of sqldb",
      "default": kwargs['table_name']
    },
    "host": {
      "help": "Serve table as a web page at http://localhost:port. Must specify --table_name",
      "default": kwargs['host']
    },
    "port": {
      "help": "Serve table as a web page at http://localhost:port. Must specify --table_name",
      "type": int,
      "default": kwargs['port']
    },
    "root-dir": {
      "help": "Root directory with subdirs of js, css, and img",
      "default": kwargs['root_dir']
    },
    "config": {
      "help": "JSON file containing configuration. See https://datatables.net/reference/option/ for options. Relative paths are relative to root-dir.",
      "default": kwargs['config']
    },
    "render": {
      "help": "Javascript file with DataTables rendering functions. Relative paths are relative to root-dir.",
      "default": kwargs['render']
    },
    "json-head": {
      "metavar": "FILE",
      "help": "JSON file containing array of header names. Ignored if sqldb given. Requires --json-body to be given.",
      "default": kwargs['json_head']
    },
    "json-body": {
      "metavar": "FILE",
      "help": "JSON array with (row) arrays of length header. Ignored if sqldb given. Requires --json-header to be given.",
      "default": kwargs['json_body']
    }
  }
