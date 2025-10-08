import tableui

def cli():
  import argparse

  def get_keywords(func):
    import inspect
    """Return a dict of (reversed) keyword arguments from a function."""
    spec = inspect.getfullargspec(func)
    keys = reversed(spec.args)
    values = reversed(spec.defaults)
    return {k: v for k, v in zip(keys, values)}

  kwargs = get_keywords(tableui.serve)

  clargs = {
    "sqldb": {
      "help": "File containing SQL database",
      "default": kwargs['sqldb']
    },
    "table-name": {
      "help": "Name of table in sqldb. Defaults to SQLDB without extension of sqldb",
      "default": kwargs['table_name']
    },
    "table-meta": {
      "metavar": "FILE",
      "help": "JSON dict with metadata keys. Passed to render function as config.tableUI.tableMetadata."
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
    "js-render": {
      "help": "Javascript file with DataTables metadata and column rendering functions. Path is relative to root-dir.",
      "default": kwargs['js_render']
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
    },
    "debug": {
      "help": "Verbose logging.",
      "action": "store_true",
      "default": False
    }
  }

  parser = argparse.ArgumentParser()
  for k, v in clargs.items():
    parser.add_argument(f'--{k}', **v)

  args = vars(parser.parse_args())

  return args
