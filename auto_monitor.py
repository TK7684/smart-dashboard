"""
Shopee Dashboard Auto-Monitor
==============================
Automatically detects new files and runs the data pipeline
Can be scheduled to run daily or run continuously as a watcher
"""

import os
import time
import json
import hashlib
from pathlib import Path
from datetime import datetime
import subprocess
import sys

# Configuration
BASE_DIR = Path(r"C:\Projects\Shopee-dashboard")
CONFIG_FILE = BASE_DIR / "monitor_config.json"
LOG_FILE = BASE_DIR / "pipeline.log"

# Folders to monitor
FOLDERS = {
    'orders': BASE_DIR / "Shopee orders",
    'ads': BASE_DIR / "Shopee Ad",
    'live': BASE_DIR / "Shopee Live",
    'video': BASE_DIR / "Shopee Video"
}


def log(message):
    """Write to log file with timestamp"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] {message}"
    print(log_entry)

    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_entry + '\n')


def get_file_hash(filepath):
    """Get MD5 hash of file for change detection"""
    try:
        with open(filepath, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    except:
        return None


def load_config():
    """Load or create configuration"""
    default_config = {
        'last_run': None,
        'file_hashes': {},
        'auto_process': True,
        'check_interval_minutes': 30
    }

    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
                # Merge with defaults
                for key in default_config:
                    if key not in config:
                        config[key] = default_config[key]
                return config
        except:
            pass

    return default_config


def save_config(config):
    """Save configuration"""
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2, default=str)


def scan_folders():
    """Scan all folders and return file info"""
    files_info = {}

    for folder_name, folder_path in FOLDERS.items():
        if not folder_path.exists():
            log(f"⚠️ Folder not found: {folder_path}")
            continue

        files_info[folder_name] = {}

        for ext in ['*.xlsx', '*.csv']:
            for filepath in folder_path.glob(ext):
                if 'desktop.ini' in filepath.name.lower():
                    continue

                file_hash = get_file_hash(filepath)
                mtime = datetime.fromtimestamp(filepath.stat().st_mtime)

                files_info[folder_name][filepath.name] = {
                    'hash': file_hash,
                    'modified': mtime.isoformat(),
                    'size': filepath.stat().st_size
                }

    return files_info


def check_for_new_files(config):
    """Check for new or modified files"""
    current_files = scan_folders()
    old_hashes = config.get('file_hashes', {})

    new_files = []
    modified_files = []

    for folder_name, files in current_files.items():
        old_folder_files = old_hashes.get(folder_name, {})

        for filename, info in files.items():
            if filename not in old_folder_files:
                new_files.append(f"{folder_name}/{filename}")
            elif info['hash'] != old_folder_files.get(filename, {}).get('hash'):
                modified_files.append(f"{folder_name}/{filename}")

    return new_files, modified_files, current_files


def run_pipeline():
    """Run the data pipeline script"""
    log("🚀 Starting data pipeline...")

    try:
        result = subprocess.run(
            [sys.executable, str(BASE_DIR / "data_pipeline.py")],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'
        )

        if result.returncode == 0:
            log("✅ Pipeline completed successfully")
            return True
        else:
            log(f"❌ Pipeline failed: {result.stderr}")
            return False

    except Exception as e:
        log(f"❌ Error running pipeline: {e}")
        return False


def process_once():
    """Run a single check and process if needed"""
    log("=" * 60)
    log("🔍 Checking for file changes...")

    config = load_config()
    new_files, modified_files, current_files = check_for_new_files(config)

    if new_files:
        log(f"📁 New files detected: {len(new_files)}")
        for f in new_files:
            log(f"   + {f}")

    if modified_files:
        log(f"📝 Modified files detected: {len(modified_files)}")
        for f in modified_files:
            log(f"   ~ {f}")

    if new_files or modified_files:
        log("🔄 Processing new/modified files...")

        if run_pipeline():
            # Update config after successful processing
            config['file_hashes'] = current_files
            config['last_run'] = datetime.now().isoformat()
            save_config(config)
            log("💾 Configuration updated")
    else:
        log("✓ No new or modified files detected")

    return new_files, modified_files


def run_continuous():
    """Run continuous monitoring"""
    config = load_config()
    interval = config.get('check_interval_minutes', 30)

    log(f"🔄 Starting continuous monitoring (checking every {interval} minutes)")
    log("Press Ctrl+C to stop")

    try:
        while True:
            process_once()
            log(f"😴 Sleeping for {interval} minutes...")
            time.sleep(interval * 60)
    except KeyboardInterrupt:
        log("🛑 Monitoring stopped by user")


def run_scheduled():
    """Run as a scheduled task (for Windows Task Scheduler or cron)"""
    log("⏰ Scheduled run started")

    config = load_config()

    # Force processing regardless of file changes
    log("🔄 Running pipeline (scheduled mode)...")

    if run_pipeline():
        # Update file hashes after processing
        current_files = scan_folders()
        config['file_hashes'] = current_files
        config['last_run'] = datetime.now().isoformat()
        save_config(config)

    log("⏰ Scheduled run completed")


# ==========================================
# MAIN ENTRY POINT
# ==========================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Shopee Dashboard Auto-Monitor')
    parser.add_argument('--mode', choices=['once', 'continuous', 'scheduled'],
                        default='once', help='Running mode')
    parser.add_argument('--interval', type=int, default=30,
                        help='Check interval in minutes (for continuous mode)')
    parser.add_argument('--force', action='store_true',
                        help='Force pipeline run regardless of file changes')

    args = parser.parse_args()

    # Update interval if specified
    if args.interval:
        config = load_config()
        config['check_interval_minutes'] = args.interval
        save_config(config)

    # Run in specified mode
    if args.mode == 'continuous':
        run_continuous()
    elif args.mode == 'scheduled':
        run_scheduled()
    elif args.force:
        log("🔄 Force mode: Running pipeline regardless of changes...")
        run_pipeline()
        config = load_config()
        config['file_hashes'] = scan_folders()
        config['last_run'] = datetime.now().isoformat()
        save_config(config)
    else:
        process_once()


# ==========================================
# WINDOWS TASK SCHEDULER SETUP
# ==========================================
"""
To set up automatic daily running on Windows:

1. Open Task Scheduler
2. Create Basic Task:
   - Name: "Shopee Dashboard Pipeline"
   - Trigger: Daily at 6:00 AM
   - Action: Start a Program
   - Program: python
   - Arguments: "C:\Projects\Shopee-dashboard\auto_monitor.py" --mode scheduled
   - Start in: "C:\Projects\Shopee-dashboard"

3. For continuous monitoring, use:
   python auto_monitor.py --mode continuous --interval 30

4. For one-time check:
   python auto_monitor.py --mode once

5. Force pipeline run:
   python auto_monitor.py --force
"""

# ==========================================
# ALTERNATIVE: FILE SYSTEM WATCHER
# ==========================================
"""
For real-time file watching (requires watchdog package):

pip install watchdog

Then use this code:

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class ShopeeFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            log(f"New file detected: {event.src_path}")
            run_pipeline()

    def on_modified(self, event):
        if not event.is_directory:
            log(f"File modified: {event.src_path}")
            run_pipeline()

def watch_realtime():
    event_handler = ShopeeFileHandler()
    observer = Observer()

    for folder in FOLDERS.values():
        observer.schedule(event_handler, str(folder), recursive=False)

    observer.start()
    log("👁️ Real-time file watching started...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
"""
