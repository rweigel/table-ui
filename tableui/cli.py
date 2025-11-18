def cli():
  import argparse

  # Define the text for the header
  description = """
  Serve a table UI application using Uvicorn.
  --------------------
  Example usage:
    python serve.py --config conf/demo.json
    python serve.py --config conf/demos.json

  Pass additional Uvicorn arguments as needed
    python serve.py --config conf/demos.json [Uvicorn options]
  See
    python -m uvicorn --help
  """

  import utilrsw.uvicorn
  clargs_uvicorn = utilrsw.uvicorn.cli()

  config_help = "Path to JSON configuration file. Relative paths are "
  config_help += "interpreted as relative to current working directory "
  config_help += "server.py executed from."

  clargs = {
    "config": {
      "help": config_help,
      "type": str,
      "default": "conf/demo.json"
    },
    **clargs_uvicorn,
    "debug": {
      "help": "Verbose logging.",
      "action": "store_true",
      "default": False
    },
    "log-level": {
      "help": "Set logging level (e.g., DEBUG, INFO, WARNING, ERROR).",
      "type": str,
      "default": None
    }
  }

  parser_kwargs = {
    "description": description,
    "formatter_class": argparse.RawDescriptionHelpFormatter
  }

  parser = argparse.ArgumentParser()
  parser = argparse.ArgumentParser(**parser_kwargs)

  for k, v in clargs.items():
    parser.add_argument(f'--{k}', **v)

  configs = utilrsw.uvicorn.cli(parser=parser)

  return configs
