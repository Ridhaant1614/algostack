@echo off
REM AlgoStack Crash Diagnostics
REM Run this to find why UnifiedDash keeps crashing
REM Double-click this file OR run from PowerShell

cd /d "%~dp0"
echo ============================================================
echo  AlgoStack UnifiedDash Crash Diagnostics
echo ============================================================
echo.

REM Force UTF-8 in this terminal session
chcp 65001 > nul 2>&1
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

echo Running diagnostics...
echo Output will also be saved to: diagnose_output.txt
echo.

python -X utf8 diagnose_dash.py 2>&1 | tee diagnose_output.txt

echo.
echo ============================================================
echo  Done. Check diagnose_output.txt for the full report.
echo  Send that file to support if the issue is unclear.
echo ============================================================
pause
