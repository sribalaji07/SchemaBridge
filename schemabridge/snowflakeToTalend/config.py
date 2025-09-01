import json
import os

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), 'config.json')

config = {}
if os.path.exists(CONFIG_FILE_PATH):
    with open(CONFIG_FILE_PATH, 'r') as config_file:
        config = json.load(config_file)
else:
    raise FileNotFoundError(f"Config file not found at {CONFIG_FILE_PATH}")

for key, value in config.items():
    globals()[key] = value