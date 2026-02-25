#!/bin/bash
# start_watcher.sh - File watcher for Shopee Dashboard
# Watches source folders and triggers pipeline when new files are added

PROJECT_DIR="/home/tk578/Shopee-dashboard"
SCRIPT_DIR="$PROJECT_DIR/scripts"

# Watch all source folders (Shopee + TikTok) and trigger pipeline on new files
inotifywait -m -r -e create,close_write,moved_to \
    --format "%w%f %e" \
    "$PROJECT_DIR/Shopee_orders" \
    "$PROJECT_DIR/Shopee_Ad" \
    "$PROJECT_DIR/Shopee_Live" \
    "$PROJECT_DIR/Shopee_Video" \
    "$PROJECT_DIR/Tiktok_Live" \
    "$PROJECT_DIR/Tiktok_Video" 2>/dev/null | \
while read -r file event; do
    echo "Detected: $file ($event)"
    "$SCRIPT_DIR/pipeline_updater.sh" "$file" "$event"
done
