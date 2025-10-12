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
    "config": {
      "help": "Path to JSON configuration file. Relative paths are interpreted as relative to current working directory server.py executed from.",
      "default": kwargs['config']
    },
    "host": {
      "help": "Serve table as a web page at http://host:port.",
      "default": kwargs['host']
    },
    "port": {
      "help": "Serve table as a web page at http://host:port.",
      "type": int,
      "default": kwargs['port']
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
