@echo off
chcp 65001 >nul
echo ============================================
echo   SHOPEE DASHBOARD DATA PIPELINE
echo ============================================
echo.

cd /d "C:\Projects\Shopee-dashboard"

echo Running data pipeline...
echo.

python data_pipeline.py

echo.
echo ============================================
echo   Pipeline completed!
echo   Output files in: processed_data folder
echo ============================================
echo.

pause
