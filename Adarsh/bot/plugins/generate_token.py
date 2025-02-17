import random
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyrogram import Client
from Adarsh.bot import StreamBot
from Adarsh.utils.database import Database
from Adarsh.vars import Var

# Database setup
db = Database(Var.DATABASE_URL, "tokens")

# Function to generate a random daily token
def generate_token(length=8):
    characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return ''.join(random.choice(characters) for _ in range(length))

# Scheduler to reset token daily
scheduler = AsyncIOScheduler()

async def reset_daily_token():
    token = generate_token()
    now = datetime.utcnow()
    expiration = now + timedelta(days=1)

    # Save the token to MongoDB
    await db.tokens.update_one(
        {"type": "daily"},
        {"$set": {"token": token, "expiration": expiration}},
        upsert=True
    )

    # Send token to the specified Telegram channel
    channel_id = -1001767225628  # Replace with your channel ID
    await StreamBot.send_message(
        chat_id=channel_id,
        text=f"🔑 Today's Token: `{token}` (Expires at 12 AM UTC)",
        parse_mode="markdown"
    )

# Schedule the reset function at midnight
scheduler.add_job(reset_daily_token, "cron", hour=0, minute=0)
scheduler.start()
