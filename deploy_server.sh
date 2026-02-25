#!/bin/bash
# Shopee Dashboard Deployment Script
# Run this on your 24/7 server (100.66.69.21)

set -e

echo "============================================"
echo "  SHOPEE DASHBOARD - SERVER DEPLOYMENT"
echo "============================================"

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed"
    exit 1
fi

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "ERROR: Docker Compose is not installed"
    exit 1
fi

# Build and start
echo ""
echo "Building Docker image..."
docker compose build

echo ""
echo "Starting dashboard..."
docker compose up -d

echo ""
echo "Waiting for dashboard to start..."
sleep 5

# Check if running
if docker compose ps | grep -q "Up"; then
    echo "Dashboard is running!"
else
    echo "ERROR: Dashboard failed to start"
    docker compose logs
    exit 1
fi

echo ""
echo "============================================"
echo "  DASHBOARD ACCESS:"
echo "============================================"
echo "  Local:    http://localhost:8501"
echo "  Tailscale: http://100.66.69.21:8501"
echo ""
echo "To enable public access via Tailscale Funnel:"
echo "  tailscale funnel 443 --bg http://localhost:8501"
echo ""
echo "Useful commands:"
echo "  docker compose logs -f    # View logs"
echo "  docker compose down       # Stop dashboard"
echo "  docker compose restart    # Restart dashboard"
echo "============================================"
