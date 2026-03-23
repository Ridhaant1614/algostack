@echo off
REM ============================================================
REM  AlgoStack v9.0 | Author: Ridhaant Ajoy Thackur
REM  start_all.bat — Start everything via autohealer
REM
REM  15 processes managed automatically:
REM    Equity (7):    Algofinal, UnifiedDash, Scanner1/2/3, XOptimizer, BestXTrader
REM    Commodity (4): CommodityEngine, CommScanner1/2/3  [weekdays only]
REM    Crypto (4):    CryptoEngine, CryptoScanner1/2/3   [24/7 incl. weekends]
REM
REM  Single ngrok tunnel -> port 8055 (unified dashboard)
REM  Same URL sent to all 3 Telegram bots at startup.
REM
REM  Usage: Double-click this file or run in PowerShell:
REM    .\start_all.bat
REM ============================================================
echo.
echo   AlgoStack v9.0 ^| Author: Ridhaant Ajoy Thackur
echo   Equity + Commodity (MCX) + Crypto (Binance)
echo   Starting 15 processes via autohealer...
echo.

cd /d "%~dp0"

if not exist .env (
    echo.
    echo  FIRST TIME SETUP:
    echo  Copy .env.template to .env and verify your tokens:
    echo    copy .env.template .env
    echo.
    echo  The following tokens are pre-configured in .env.template:
    echo    Equity bot:    7587307352:AAG6...  (chat IDs: 1376513391, 793674804)
    echo    Commodity bot: 8340570160:AAHG...
    echo    Crypto bot:    8710104039:AAGu...
    echo.
    echo  Required: ngrok authtokens (set NGROK_AUTHTOKEN_EQUITY in .env)
    echo.
    pause
    exit /b 1
)

if not exist logs mkdir logs
if not exist levels mkdir levels
if not exist trade_logs mkdir trade_logs
if not exist trade_analysis mkdir trade_analysis
if not exist sweep_results mkdir sweep_results

echo  Starting AlgoStack v9.0...
python autohealer.py

pause
