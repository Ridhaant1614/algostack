# -*- coding: utf-8 -*-
"""
diagnose_dash.py - AlgoStack UnifiedDash Crash Diagnostics
Run with: python diagnose_dash.py
"""
import sys, os, traceback, subprocess

os.environ['PYTHONIOENCODING'] = 'utf-8'
os.environ['PYTHONUTF8'] = '1'
if hasattr(sys.stdout, 'reconfigure'):
    try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except: pass

SEP = "=" * 60
print(SEP)
print("AlgoStack UnifiedDash Diagnostics v10.9")
print(SEP)
print("Python:", sys.version)
print("CWD:", os.getcwd())
print()

for fname in ['unified_dash_v3.py', 'dash_launcher.py', 'autohealer.py']:
    exists = os.path.exists(fname)
    size = os.path.getsize(fname) if exists else 0
    status = f"OK ({size:,} bytes)" if exists else "MISSING!"
    print(f"  {'[OK]' if exists else '[!!]'} {fname}: {status}")

print()
import importlib
pkgs = [('dash','dash'),('plotly','plotly'),('pytz','pytz'),
        ('flask','flask'),('waitress','waitress'),('flask_compress','flask_compress')]
print("Package check:")
for name, mod in pkgs:
    try:
        m = importlib.import_module(mod)
        ver = getattr(m, '__version__', '?')
        print(f"  [OK] {name} {ver}")
    except ImportError:
        marker = "[!!]" if name in ('dash','plotly','pytz','flask') else "[--]"
        print(f"  {marker} {name}: not installed")

print()
print("Trying to run dash_launcher.py (20 second timeout)...")
print(SEP)
try:
    result = subprocess.run(
        [sys.executable, '-X', 'utf8', '-u', 'dash_launcher.py'],
        capture_output=True, text=True,
        encoding='utf-8', errors='replace', timeout=20
    )
    print("Exit code:", result.returncode)
    if result.stdout:
        print("Output:")
        print(result.stdout[:3000])
    if result.stderr:
        print("Errors:")
        print(result.stderr[:2000])
    if result.returncode != 0:
        print()
        print("CRASH DETECTED. Send this output for support.")
except subprocess.TimeoutExpired:
    print("Timeout after 20s - dashboard likely started OK!")
    print("Try: python autohealer.py")
except Exception as e:
    print(f"Could not run test: {e}")

print(SEP)
print("Save this output and send it for support.")
