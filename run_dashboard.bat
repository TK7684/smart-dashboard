@echo off
chcp 65001 >nul
echo ============================================
echo   SHOPEE EXECUTIVE DASHBOARD
echo   Shareable on Private Network
echo ============================================
echo.

cd /d "C:\Projects\Shopee-dashboard"

echo Checking dependencies...
python -m pip install streamlit plotly duckdb pandas openpyxl -q

echo.
echo Starting Streamlit dashboard...
echo.
echo Dashboard will be available at:
echo   Local:   http://localhost:8501
echo   Network: http://100.66.69.21:8501
echo.
echo Press Ctrl+C to stop the server
echo ============================================

python -m streamlit run dashboard.py --server.address 0.0.0.0 --server.port 8501

pause
