@echo off
REM ==============================
REM Math Options Dashboard Launcher
REM ==============================

REM --- Check if PYTHON_EXE env var is already set ---
if not defined PYTHON_EXE (
    echo [!] PYTHON_EXE is not set in environment variables.
    echo     Please set it permanently in Windows Environment Variables,
    echo     or edit this file and add:
    echo     set "PYTHON_EXE=C:\Path\To\Python.exe"
    echo.
    pause
    exit /b 1
)

REM --- Navigate to project directory (where this BAT file resides) ---
cd /d "%~dp0"

REM --- Detect local IP address (IPv4 only) ---
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /i "IPv4"') do (
    set "LOCAL_IP=%%a"
    goto :foundip
)
:foundip
set "LOCAL_IP=%LOCAL_IP: =%"

echo Detected local IP: %LOCAL_IP%
echo.

REM --- Optional: Environment variables (credentials not used in code) ---
REM set "DHAN_CLIENT_ID=your_client_id"
REM set "DHAN_ACCESS_TOKEN=your_access_token"
REM set "DHAN_ACCESS_KEY=your_access_key"

REM --- Launch Dash App with logging ---
echo Starting Math Options Dashboard on http://%LOCAL_IP%:8050
echo Log file: app.log
echo =========================================
"%PYTHON_EXE%" Dash_app.py > app.log 2>&1

REM --- Keep window open after program exits ---
echo.
echo =========================================
echo Dash App has stopped. Logs are saved in app.log
pause >nul