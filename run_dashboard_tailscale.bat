@echo off
chcp 65001 >nul
echo ============================================
echo   SHOPEE DASHBOARD - TAILSCALE FUNNEL
echo   Public Access Setup
echo ============================================
echo.

cd /d "C:\Projects\Shopee-dashboard"

echo Step 1: Checking Tailscale...
tailscale status >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Tailscale is not running!
    echo Please install and login to Tailscale first:
    echo https://tailscale.com/download
    pause
    exit /b 1
)
echo OK: Tailscale is running

echo.
echo Step 2: Checking dependencies...
python -m pip install streamlit plotly duckdb pandas openpyxl -q

echo.
echo Step 3: Starting Streamlit Dashboard...
echo.
start /b python -m streamlit run dashboard.py --server.address 0.0.0.0 --server.port 8501

echo Waiting for dashboard to start...
timeout /t 5 /nobreak >nul

echo.
echo Step 4: Starting Tailscale Funnel...
echo.
echo ============================================
echo   YOUR PUBLIC URLS:
echo ============================================
echo.

tailscale funnel 8501

echo.
echo Funnel stopped. Press any key to exit.
pause >nul
