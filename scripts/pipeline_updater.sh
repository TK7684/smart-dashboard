#!/bin/bash
# pipeline_updater.sh - Handles Docker lifecycle and pipeline execution
# Triggered by inotify when new files are added to source folders

set -e

# Configuration
PROJECT_DIR="$HOME/Shopee-dashboard"
LOG_FILE="$PROJECT_DIR/pipeline.log"
LOCK_FILE="/tmp/shopee_pipeline.lock"
DEBOUNCE_SECONDS=5

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Check if triggered with file path (from inotify)
TRIGGER_FILE="${1:-}"
TRIGGER_EVENT="${2:-}"

if [ -n "$TRIGGER_FILE" ]; then
    log "Triggered by: $TRIGGER_FILE ($TRIGGER_EVENT)"
fi

# Debounce: Wait for file operations to settle
log "Waiting $DEBOUNCE_SECONDS seconds for file operations to complete..."
sleep $DEBOUNCE_SECONDS

# Check if already running (lock file)
if [ -f "$LOCK_FILE" ]; then
    log "Pipeline already running (lock file exists), skipping..."
    exit 0
fi

# Create lock file
touch "$LOCK_FILE"
log "Acquired lock, starting update..."

# Cleanup function
cleanup() {
    rm -f "$LOCK_FILE"
    log "Released lock"
}
trap cleanup EXIT

# Check if dashboard is running
if ! docker compose -f "$PROJECT_DIR/docker-compose.yml" ps | grep -q "Up"; then
    log "Dashboard is not running, starting it first..."
    docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d
    sleep 5
fi

# Stop Docker container (releases database lock)
log "Stopping dashboard container..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" stop

# Wait for clean shutdown
sleep 2

# Verify container is stopped
if docker compose -f "$PROJECT_DIR/docker-compose.yml" ps | grep -q "Up"; then
    log "WARNING: Container still running, forcing stop..."
    docker compose -f "$PROJECT_DIR/docker-compose.yml" down
    sleep 2
fi

# Run the data pipeline using Docker (server doesn't have Python with dependencies)
log "Running data pipeline..."
cd "$PROJECT_DIR"

# Use the existing shopee-dashboard image to run the pipeline
if docker run --rm -v "$PROJECT_DIR:/app" -w /app --entrypoint python shopee-dashboard-shopee-dashboard data_pipeline.py >> "$LOG_FILE" 2>&1; then
    log "Pipeline completed successfully"
else
    log "ERROR: Pipeline failed - check log for details"
fi

# Restart Docker container
log "Restarting dashboard container..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d

# Wait for dashboard to be ready
log "Waiting for dashboard to start..."
sleep 5

# Health check
HEALTH_URL="http://localhost:8501/_stcore/health"
MAX_RETRIES=6
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -sf "$HEALTH_URL" > /dev/null 2>&1; then
        log "Dashboard is healthy and ready"
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    log "Health check attempt $RETRY_COUNT/$MAX_RETRIES failed, retrying..."
    sleep 5
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    log "WARNING: Dashboard health check failed after $MAX_RETRIES attempts"
fi

log "Update complete"
echo "========================================" >> "$LOG_FILE"
