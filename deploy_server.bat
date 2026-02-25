@echo off
REM Shopee Dashboard Deployment Script
REM Run this on your 24/7 server (100.66.69.21)

echo ============================================
echo   SHOPEE DASHBOARD - SERVER DEPLOYMENT
echo ============================================

REM Check Docker
docker --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker is not installed
    exit /b 1
)

REM Build and start
echo.
echo Building Docker image...
docker compose build

echo.
echo Starting dashboard...
docker compose up -d

echo.
echo Waiting for dashboard to start...
timeout /t 5 /nobreak >nul

REM Check if running
docker compose ps | findstr "Up" >nul
if errorlevel 1 (
    echo ERROR: Dashboard failed to start
    docker compose logs
    exit /b 1
)

echo.
echo ============================================
echo   DASHBOARD ACCESS:
echo ============================================
echo   Local:    http://localhost:8501
echo   Tailscale: http://100.66.69.21:8501
echo.
echo To enable public access via Tailscale Funnel:
echo   tailscale serve https / http://localhost:8501
echo   tailscale funnel --bg --https=443
echo.
echo Useful commands:
echo   docker compose logs -f    # View logs
echo   docker compose down       # Stop dashboard
echo   docker compose restart    # Restart dashboard
echo ============================================
