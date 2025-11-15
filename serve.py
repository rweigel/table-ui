import copy
import logging
import tableui

args = tableui.cli()

# configure basic logging so messages are printed to the console
logging.basicConfig(level=logging.DEBUG if args.get('debug') else logging.INFO)
logger = logging.getLogger('utilrsw.uvicorn')
if args.get('debug'):
  logger.setLevel(logging.DEBUG)

logger.info("Starting table UI server with configuration:")

# prepare server config by copying args and removing keys not intended for the server
config_server = copy.deepcopy(args)
for key in ['config', 'debug']:
  config_server.pop(key, None)

tableui.run(config_server=config_server, config_app=args['config'])