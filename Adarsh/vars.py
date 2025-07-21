import os
import random
from os import getenv, environ
from dotenv import load_dotenv

load_dotenv()


class Var(object):
    MULTI_CLIENT = False
    API_ID = int(getenv('API_ID', ''))
    API_HASH = str(getenv('API_HASH', ''))
    BOT_TOKEN = str(getenv('BOT_TOKEN', ''))
    name = str(getenv('name', 'Nobita-Stream-Bot'))
    SLEEP_THRESHOLD = int(getenv('SLEEP_THRESHOLD', '60'))
    WORKERS = int(getenv('WORKERS', '4'))
    BIN_CHANNEL = int(getenv('BIN_CHANNEL', ''))
    DB_CHANNEL = int(getenv('DB_CHANNEL', ''))
    DISABLE_CHANNEL_BUTTON = os.environ.get("DISABLE_CHANNEL_BUTTON", None) == 'True'
    PROTECT_CONTENT = os.environ.get('PROTECT_CONTENT', "True") == "True"
    CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", None)
    PORT = int(getenv('PORT', 8080))
    BIND_ADDRESS = str(getenv('WEB_SERVER_BIND_ADDRESS', '0.0.0.0'))
    PING_INTERVAL = int(environ.get("PING_INTERVAL", "1200"))  # 20 minutes
    OWNER_ID = set(int(x) for x in os.environ.get("OWNER_ID", "").split() if x.isdigit())
    NO_PORT = bool(getenv('NO_PORT', False))
    APP_NAME = None
    OWNER_USERNAME = str(getenv('OWNER_USERNAME', 'NobiDeveloperr'))

    if 'DYNO' in environ:
        ON_HEROKU = True
        APP_NAME = str(getenv('APP_NAME'))
    else:
        ON_HEROKU = False

    _FQDN_LIST = ["stream.nextpulse.workers.dev"] + [f"stream{i}.nextpulse.workers.dev" for i in range(1, 10)]

    DATABASE_URL = str(getenv('DATABASE_URL', ''))
    UPDATES_CHANNEL = str(getenv('UPDATES_CHANNEL', None))
    BANNED_CHANNELS = list(set(int(x) for x in str(getenv("BANNED_CHANNELS", "")).split() if x.isdigit()))

    # ✅ Per-file mapping: file_id/message_id → fqdn
    _file_fqdn_map = {}

    @classmethod
    def get_fqdn_for_file(cls, file_id: str) -> str:
        """Returns consistent random FQDN for a file_id within batch."""
        if file_id not in cls._file_fqdn_map:
            cls._file_fqdn_map[file_id] = random.choice(cls._FQDN_LIST)
        return cls._file_fqdn_map[file_id]

    @classmethod
    def get_url_for_file(cls, file_id: str) -> str:
        return f"https://{cls.get_fqdn_for_file(file_id)}/"

    @classmethod
    def reset_batch(cls):
        """Call this before starting a new batch to clear file-to-stream mapping."""
        cls._file_fqdn_map.clear()


# Instantiate
Var = Var()
