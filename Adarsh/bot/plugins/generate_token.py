import random
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from Adarsh.utils.database import Database
from Adarsh.vars import Var

db = Database(Var.DATABASE_URL, "tokens")

# Function to generate a random daily token
def generate_token(length=8):
    characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return ''.join(random.choice(characters) for _ in range(length))

# Scheduler to reset token daily
scheduler = BackgroundScheduler()

def reset_daily_token():
    token = generate_token()
    now = datetime.utcnow()
    expiration = now + timedelta(days=1)

    db.tokens.update_one(
        {"type": "daily"},
        {"$set": {"token": token, "expiration": expiration}},
        upsert=True
    )

# Fetch the current token from the database
def get_current_token():
    token_data = db.tokens.find_one({"type": "daily"})
    return token_data["token"] if token_data else None

# Schedule the reset function at midnight
scheduler.add_job(reset_daily_token, "cron", hour=0, minute=0)
scheduler.start()
