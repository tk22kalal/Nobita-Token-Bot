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
        
    # ✅ Cloudflare worker for permanent links
    _FQDN_LIST = ["stream14.nextpulse.workers.dev"]
    DATABASE_URL = str(getenv('DATABASE_URL', ''))
    UPDATES_CHANNEL = str(getenv('UPDATES_CHANNEL', None))
    BANNED_CHANNELS = list(set(int(x) for x in str(getenv("BANNED_CHANNELS", "")).split() if x.isdigit()))
    _file_fqdn_map = {}  # file_id -> fqdn
    _current_fqdn = random.choice(_FQDN_LIST)
    _current_repeat = 0
    
    # Default
    FQDN = _current_fqdn
    URL = f"https://{FQDN}/"
    
    @classmethod
    def get_fqdn_for_file(cls, file_id: str) -> str:
        if file_id not in cls._file_fqdn_map:
            # Since only one FQDN exists, no need for rotation
            cls._file_fqdn_map[file_id] = cls._current_fqdn
        return cls._file_fqdn_map[file_id]
    
    @classmethod
    def get_url_for_file(cls, file_id: str) -> str:
        return f"https://{cls.get_fqdn_for_file(file_id)}/"
    
    @classmethod
    def reset_batch(cls):
        pass

# Instantiate the config object
Var = Var()
