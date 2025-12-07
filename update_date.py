import os
import sys
import time
from datetime import datetime, timedelta
import pytz

IST = pytz.timezone('Asia/Kolkata')
# Update every 6 hours: at 0:00, 6:00, 12:00, 18:00 IST
UPDATE_HOURS = [0, 6, 12, 18]
UPDATE_MINUTE = 0

def update_date_file():
    """Update date.txt with current IST timestamp"""
    now_ist = datetime.now(IST)
    timestamp = now_ist.strftime('%Y-%m-%d %H:%M:%S IST')
    
    with open('date.txt', 'w') as f:
        f.write(f"Last updated: {timestamp}\n")
    
    print(f"[{timestamp}] date.txt updated successfully")
    return timestamp

def get_next_run_time():
    """Calculate seconds until next scheduled update (0, 6, 12, or 18 hours IST)"""
    now = datetime.now(IST)
    
    # Find the next update hour
    next_hour = None
    for hour in UPDATE_HOURS:
        target_time = now.replace(hour=hour, minute=UPDATE_MINUTE, second=0, microsecond=0)
        if now < target_time:
            next_hour = hour
            next_run = target_time
            break
    
    # If all today's times have passed, schedule for first time tomorrow
    if next_hour is None:
        next_run = now.replace(hour=UPDATE_HOURS[0], minute=UPDATE_MINUTE, second=0, microsecond=0)
        next_run += timedelta(days=1)
    
    time_diff = (next_run - now).total_seconds()
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Next update scheduled for: {next_run.strftime('%Y-%m-%d %H:%M:%S')} IST")
    print(f"Waiting {time_diff/3600:.2f} hours ({int(time_diff)} seconds)")
    
    return time_diff

def main():
    """Main scheduler loop or one-time update"""
    # Check if running in one-time mode (for GitHub Actions)
    if len(sys.argv) > 1 and sys.argv[1] == '--once':
        print("Running in one-time update mode...")
        update_date_file()
        return
    
    # Continuous scheduler mode
    print("=" * 60)
    print("Date.txt Auto-Updater Started")
    print(f"Schedule: 4 times daily at {', '.join([f'{h:02d}:{UPDATE_MINUTE:02d}' for h in UPDATE_HOURS])} IST")
    print("=" * 60)
    
    while True:
        try:
            # Calculate wait time until next scheduled update
            wait_seconds = get_next_run_time()
            
            # Sleep until the scheduled time
            time.sleep(wait_seconds)
            
            # Update the file
            update_date_file()
            
            # Small delay to prevent multiple updates in the same minute
            time.sleep(60)
            
        except KeyboardInterrupt:
            print("\nScheduler stopped by user")
            break
        except Exception as e:
            print(f"Error: {e}")
            # Wait 1 minute before retrying on error
            time.sleep(60)

if __name__ == "__main__":
    main()
