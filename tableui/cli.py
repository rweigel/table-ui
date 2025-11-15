def cli():
  import argparse

  # Define the text for the header
  description = """
  Serve a table UI application using Uvicorn.
  --------------------
  Example usage:
    python serve.py --config conf/demo.json
    python serve.py --config conf/demos.json
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
      "required": True
    },
    **clargs_uvicorn,
    "debug": {
      "help": "Verbose logging.",
      "action": "store_true",
      "default": False
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

  args = utilrsw.uvicorn.cli(parser=parser)

  return args
