"""
State management for incremental data loading.
Tracks which dates have been processed to enable idempotent pipeline runs.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from .config import LAST_RUN_FILE


def get_last_run_date():
    """
    Read the last successfully processed date from metadata file.
    
    Returns:
        datetime.date: Last processed date, or None if never run
    """
    # Check if metadata file exists
    if not LAST_RUN_FILE.exists():
        return None
    
    # Read JSON metadata
    with open(LAST_RUN_FILE, 'r') as f:
        metadata = json.load(f)
    
    # Parse ISO date string back to date object
    last_date_str = metadata.get('last_run_date')
    if last_date_str:
        return datetime.fromisoformat(last_date_str).date()
    
    return None


def save_last_run_date(run_date):
    """
    Save the successfully processed date to metadata file.
    
    Args:
        run_date (datetime.date): Date that was just processed
    """
    # Create metadata dictionary
    metadata = {
        'last_run_date': run_date.isoformat(),
        'last_run_timestamp': datetime.now().isoformat(),
    }
    
    # Write to JSON file
    with open(LAST_RUN_FILE, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"📝 Saved metadata: Last run date = {run_date}")


def get_next_batch_date():
    """
    Determine what date the next batch should be generated for.
    
    Logic:
        - If never run before: Start 90 days ago
        - If has run before: Return the day after last run
        - Never process future dates (today is the limit)
    
    Returns:
        datetime.date: Date for next batch, or None if already caught up
    """
    last_run = get_last_run_date()
    today = datetime.now().date()
    
    if last_run is None:
        # First run: start 90 days ago
        start_date = today - timedelta(days=90)
        print(f"🆕 First run detected. Starting from {start_date}")
        return start_date
    
    # Calculate next date (day after last run)
    next_date = last_run + timedelta(days=1)
    
    # Don't process future dates
    if next_date > today:
        print(f"✅ Already caught up! Last run: {last_run}, Today: {today}")
        return None
    
    print(f"➡️  Next batch date: {next_date} (last run was {last_run})")
    return next_date
