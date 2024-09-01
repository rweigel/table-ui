import argparse

import tableui

def cli():

  clargs = tableui.cli.args()

  parser = argparse.ArgumentParser()
  for k, v in clargs.items():
    parser.add_argument(f'--{k}', **v)

  args = vars(parser.parse_args())

  # Check and update given args with additional defaults
  args = tableui.cli.defaults(args)
  return args

tableui.serve(**cli())