import os
import sys
import time
from datetime import datetime, timedelta
import pytz

IST = pytz.timezone('Asia/Kolkata')
TARGET_HOUR = 3
TARGET_MINUTE = 0

def update_date_file():
    """Update date.txt with current IST timestamp"""
    now_ist = datetime.now(IST)
    timestamp = now_ist.strftime('%Y-%m-%d %H:%M:%S IST')
    
    with open('date.txt', 'w') as f:
        f.write(f"Last updated: {timestamp}\n")
    
    print(f"[{timestamp}] date.txt updated successfully")
    return timestamp

def get_next_run_time():
    """Calculate seconds until next 3 AM IST"""
    now = datetime.now(IST)
    
    # Set target to today at 3 AM
    next_run = now.replace(hour=TARGET_HOUR, minute=TARGET_MINUTE, second=0, microsecond=0)
    
    # If 3 AM has passed today, schedule for tomorrow
    if now >= next_run:
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
    print(f"Schedule: Daily at {TARGET_HOUR:02d}:{TARGET_MINUTE:02d} IST")
    print("=" * 60)
    
    while True:
        try:
            # Calculate wait time until next 3 AM
            wait_seconds = get_next_run_time()
            
            # Sleep until 3 AM
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
