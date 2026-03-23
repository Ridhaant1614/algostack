@echo off
:: AlgoStack v10.2 — Optimised Startup Script
:: Author: Ridhaant Ajoy Thackur
:: Sets high performance power plan before starting

title AlgoStack v10.2 Startup

:: Set Windows High Performance power plan (reduces CPU throttling)
powercfg /setactive 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c 2>nul
echo [Power] High performance power plan activated

:: Disable CPU throttling on i5-12450H
powercfg /change processor-throttle-ac 100 2>nul

:: Set Python process to high priority via start /high
:: Navigate to script directory
cd /d "%~dp0"

echo.
echo  ╔═══════════════════════════════════════╗
echo  ║   AlgoStack v10.2 — Starting...       ║
echo  ║   Author: Ridhaant Ajoy Thackur        ║
echo  ║   2,352,000 calculations / 1-5 min    ║
echo  ╚═══════════════════════════════════════╝
echo.

:: Check Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found. Install Python 3.10+ and add to PATH.
    pause
    exit /b 1
)

:: Check if .env exists
if not exist ".env" (
    echo WARNING: .env not found. Copying from .env.template...
    if exist ".env.template" (
        copy ".env.template" ".env" >nul
        echo Created .env from template. Edit it to add your API keys.
    ) else (
        echo ERROR: .env.template not found.
        pause
        exit /b 1
    )
)

:: Check for GPU acceleration
python -c "import cupy; print('[GPU] CuPy available - GTX 1650 acceleration ENABLED')" 2>nul
if errorlevel 1 (
    python -c "import numba; print('[CPU] Numba JIT available - 5-10x CPU acceleration ENABLED')" 2>nul
    if errorlevel 1 (
        echo [CPU] NumPy baseline mode ^(install numba for 5-10x speedup^)
    )
)

echo.
echo Starting autohealer.py...
echo Dashboard will be available at: http://localhost:8055
echo Telegram alert will contain public URL.
echo.

:: Start with above-normal priority
start /abovenormal /b "AlgoStack" python autohealer.py

echo AlgoStack started. Press Ctrl+C in the AlgoStack window to stop.
