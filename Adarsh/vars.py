import os
from os import getenv, environ
from dotenv import load_dotenv



load_dotenv()


import random
import os
from os import getenv, environ
from urllib.parse import quote_plus

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
    PROTECT_CONTENT = True if os.environ.get('PROTECT_CONTENT', "True") == "True" else False
    CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", None)
    PORT = int(getenv('PORT', 8080))
    BIND_ADDRESS = str(getenv('WEB_SERVER_BIND_ADDRESS', '0.0.0.0'))
    PING_INTERVAL = int(environ.get("PING_INTERVAL", "1200"))  # 20 minutes
    OWNER_ID = set(int(x) for x in os.environ.get("OWNER_ID", "").split())  
    NO_PORT = bool(getenv('NO_PORT', False))
    APP_NAME = None
    OWNER_USERNAME = str(getenv('OWNER_USERNAME', 'NobiDeveloperr'))

    if 'DYNO' in environ:
        ON_HEROKU = True
        APP_NAME = str(getenv('APP_NAME'))
    else:
        ON_HEROKU = False

    # Multiple Cloudflare Worker URLs
    WORKER_DOMAINS = [
        "stream1.nextpulse.workers.dev",
        "stream2.nextpulse.workers.dev",
        "stream3.nextpulse.workers.dev",
        "stream4.nextpulse.workers.dev"
    ]

    # Select a random worker for each request
    @staticmethod
    def get_random_worker():
        return f"https://{random.choice(Var.WORKER_DOMAINS)}/"

    # Keep URL as a property so it dynamically changes
    @property
    def URL(self):
        return self.get_random_worker()

    DATABASE_URL = str(getenv('DATABASE_URL', ''))
    UPDATES_CHANNEL = str(getenv('UPDATES_CHANNEL', None))
    BANNED_CHANNELS = list(set(int(x) for x in str(getenv("BANNED_CHANNELS", "")).split()))
