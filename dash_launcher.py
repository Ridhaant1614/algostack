# -*- coding: utf-8 -*-
# dash_launcher.py - AlgoStack v10.9 - Pure ASCII wrapper
# Autohealer calls THIS file. It sets up encoding then runs unified_dash_v3.py
import sys, os, subprocess

# Step 1: Force UTF-8 before anything else
os.environ['PYTHONIOENCODING'] = 'utf-8'
os.environ['PYTHONUTF8'] = '1'
if hasattr(sys.stdout, 'reconfigure'):
    try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except: pass
if hasattr(sys.stderr, 'reconfigure'):
    try: sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except: pass

print('[LAUNCHER] dash_launcher.py v10.9 starting', flush=True)
print('[LAUNCHER] Python', sys.version, flush=True)
print('[LAUNCHER] CWD:', os.getcwd(), flush=True)

# Step 2: Check target file exists
TARGET = os.path.join(os.getcwd(), 'unified_dash_v3.py')
if not os.path.exists(TARGET):
    print('[LAUNCHER] FATAL: unified_dash_v3.py not found!', flush=True)
    sys.exit(1)

size = os.path.getsize(TARGET)
print(f'[LAUNCHER] unified_dash_v3.py: {size:,} bytes', flush=True)

# Step 3: Quick syntax check
import ast
try:
    with open(TARGET, 'r', encoding='utf-8', errors='replace') as f:
        src = f.read()
    ast.parse(src)
    print('[LAUNCHER] Syntax OK', flush=True)
except SyntaxError as e:
    print(f'[LAUNCHER] SYNTAX ERROR at line {e.lineno}: {e.msg}', flush=True)
    print(f'[LAUNCHER] Text: {e.text}', flush=True)
    sys.exit(1)
except Exception as e:
    print(f'[LAUNCHER] File read error: {e}', flush=True)
    sys.exit(1)

# Step 4: Patch the source to add encoding fix if missing
if 'PYTHONIOENCODING' not in src[:500]:
    encoding_fix = (
        "import sys as _s, os as _o\n"
        "_o.environ['PYTHONIOENCODING']='utf-8'\n"
        "_o.environ['PYTHONUTF8']='1'\n"
        "if hasattr(_s.stdout,'reconfigure'):\n"
        "    try: _s.stdout.reconfigure(encoding='utf-8',errors='replace')\n"
        "    except: pass\n"
        "if hasattr(_s.stderr,'reconfigure'):\n"
        "    try: _s.stderr.reconfigure(encoding='utf-8',errors='replace')\n"
        "    except: pass\n"
        "del _s, _o\n"
    )
    # Find insertion point - after docstring if any
    if src.startswith('"""'):
        end = src.find('"""', 3) + 3
        src = src[:end] + '\n' + encoding_fix + src[end:]
    else:
        src = encoding_fix + src
    print('[LAUNCHER] Applied encoding patch to source', flush=True)

# Step 5: Execute the dashboard
print('[LAUNCHER] Starting unified_dash_v3.py...', flush=True)
try:
    code = compile(src, TARGET, 'exec')
    exec(code, {'__name__': '__main__', '__file__': TARGET})
except SystemExit as e:
    sys.exit(e.code)
except Exception as e:
    import traceback
    print('[LAUNCHER] CRASH:', str(e), flush=True)
    print('[LAUNCHER] Full traceback:', flush=True)
    traceback.print_exc()
    sys.exit(1)
