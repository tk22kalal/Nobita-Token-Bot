from flask import Flask, render_template, request
from apscheduler.schedulers.background import BackgroundScheduler
from pyrogram import Client
import random
import datetime
from Adarsh.utils.database import Database
from Adarsh.vars import Var

app = Flask(__name__)

# Initialize database (MongoDB)
db = Database(Var.DATABASE_URL, "tokens")

# Token generation function
def generate_token(length=8):
    characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return ''.join(random.choice(characters) for _ in range(length))

# Function to reset token daily and store in MongoDB
def reset_daily_token():
    token = generate_token()
    now = datetime.datetime.utcnow()
    expiration = now + datetime.timedelta(days=1)

    # Save token to MongoDB
    db.tokens.update_one(
        {"type": "daily"},
        {"$set": {"token": token, "expiration": expiration}},
        upsert=True
    )
    
    # Send token to Telegram channel
    channel_id = Var.TOKEN_CHANNEL_ID  # Your channel ID here
    bot = Client("my_bot")
    bot.send_message(
        chat_id=channel_id,
        text=f"🔑 Today's Token: `{token}` (Expires at 12 AM UTC)",
        parse_mode="markdown"
    )

# Set the scheduler to run the token reset at midnight UTC
scheduler = BackgroundScheduler()
scheduler.add_job(func=reset_daily_token, trigger="cron", hour=0, minute=0)
scheduler.start()

# Route to display the token input page
@app.route('/')
def index():
    return render_template('req.html')

# Route to handle token validation from the form
@app.route('/submit_token', methods=['POST'])
def submit_token():
    token = request.form['token']
    
    # Validate the token against MongoDB
    token_data = db.tokens.find_one({"type": "daily"})
    if token_data and token_data["token"] == token:
        return "✅ Token Validated! Access Granted!"
    else:
        return "❌ Invalid Token. Please try again."

if __name__ == "__main__":
    app.run(debug=True)
