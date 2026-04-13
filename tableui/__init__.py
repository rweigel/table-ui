from tableui.cli import cli
from tableui.app import app
from tableui.lists2table import lists2table
from tableui.dicts2table import dicts2table
from tableui import sql

from importlib.metadata import version
__version__ = version("tableui")

import logging
logging.basicConfig()
