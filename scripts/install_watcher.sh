#!/bin/bash
# install_watcher.sh - Installs the Shopee file watcher as a systemd service

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SERVICE_NAME="shopee-watcher"

echo "============================================"
echo "  Shopee Dashboard File Watcher Installer"
echo "============================================"
echo ""

# Check if running as root or with sudo
if [ "$EUID" -ne 0 ]; then
    echo "Please run with sudo: sudo ./install_watcher.sh"
    exit 1
fi

# Get actual user (not root)
ACTUAL_USER="${SUDO_USER:-$USER}"
ACTUAL_HOME=$(getent passwd "$ACTUAL_USER" | cut -d: -f6)

echo "Project directory: $PROJECT_DIR"
echo "Service user: $ACTUAL_USER"
echo ""

# Check for inotify-tools
echo "Checking for inotify-tools..."
if ! command -v inotifywait &> /dev/null; then
    echo "inotify-tools not found. Installing..."
    apt-get update && apt-get install -y inotify-tools
else
    echo "inotify-tools is already installed"
fi

# Update paths in systemd service file
echo "Creating systemd service file..."
cat > "/etc/systemd/system/${SERVICE_NAME}.service" << EOF
[Unit]
Description=Shopee Dashboard File Watcher
After=network.target docker.service
Requires=docker.service

[Service]
Type=simple
User=${ACTUAL_USER}
WorkingDirectory=${PROJECT_DIR}
Environment=HOME=${ACTUAL_HOME}

# Watch all source folders for new/modified files
ExecStart=/bin/bash -c '\\
    inotifywait -m -r -e create,close_write,moved_to \\
        --format \"%%w%%f %%e\" \\
        "${PROJECT_DIR}/Shopee_orders" \\
        "${PROJECT_DIR}/Shopee_Ad" \\
        "${PROJECT_DIR}/Shopee_Live" \\
        "${PROJECT_DIR}/Shopee_Video" \\
    | while read file event; do \\
        echo \"Detected: \$file (\$event)\"; \\
        "${PROJECT_DIR}/scripts/pipeline_updater.sh" "\$file" "\$event"; \\
    done'

Restart=always
RestartSec=10

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

[Install]
WantedBy=multi-user.target
EOF

echo "Service file created at /etc/systemd/system/${SERVICE_NAME}.service"

# Make scripts executable
chmod +x "${PROJECT_DIR}/scripts/pipeline_updater.sh"
chmod +x "${PROJECT_DIR}/scripts/install_watcher.sh"
echo "Made scripts executable"

# Reload systemd
echo "Reloading systemd daemon..."
systemctl daemon-reload

# Enable and start service
echo "Enabling ${SERVICE_NAME} service..."
systemctl enable "${SERVICE_NAME}"

echo "Starting ${SERVICE_NAME} service..."
systemctl start "${SERVICE_NAME}"

# Wait a moment and check status
sleep 2

echo ""
echo "============================================"
echo "  Installation Complete!"
echo "============================================"
echo ""
echo "Service status:"
systemctl status "${SERVICE_NAME}" --no-pager || true
echo ""
echo "Useful commands:"
echo "  Check status:  sudo systemctl status ${SERVICE_NAME}"
echo "  View logs:     sudo journalctl -u ${SERVICE_NAME} -f"
echo "  Stop service:  sudo systemctl stop ${SERVICE_NAME}"
echo "  Restart:       sudo systemctl restart ${SERVICE_NAME}"
echo ""
echo "Pipeline logs: ${PROJECT_DIR}/pipeline.log"
echo ""
