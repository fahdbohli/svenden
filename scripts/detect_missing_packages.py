"""
Scans the workspace for Python imports and reports modules that fail to import in the current environment.
Writes a requirements_detected.txt with one package per line (best-effort names same as module names).
Usage: python3 scripts/detect_missing_packages.py
"""
import ast
import sys
from pathlib import Path
import importlib

ROOT = Path(__file__).resolve().parents[1]

# collect python files excluding caches and venvs
py_files = [p for p in ROOT.rglob('*.py') if 'site-packages' not in str(p) and '__pycache__' not in str(p) and '/.venv' not in str(p)]

modules = set()
for p in py_files:
    try:
        src = p.read_text(encoding='utf-8')
    except Exception:
        continue
    try:
        tree = ast.parse(src)
    except Exception:
        continue
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                modules.add(n.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module and not node.level:
                modules.add(node.module.split('.')[0])

# local module names (filenames) to ignore
local_names = set(p.stem for p in ROOT.rglob('*.py'))

# conservative stdlib list to ignore
stdlib_like = set(['os','sys','re','json','argparse','typing','datetime','time','math','collections','itertools','pathlib','shutil','csv','http','urllib','zoneinfo','functools','inspect','subprocess','statistics','heapq','logging','gzip','bz2','tarfile','socket','types','traceback','enum','dataclasses','random','decimal','concurrent','asyncio','base64','importlib','difflib','unicodedata','uuid'])

missing = []
for mod in sorted(modules):
    if not mod or mod.startswith('_'):
        continue
    if mod in ('__future__',):
        continue
    if mod in local_names:
        continue
    if mod in stdlib_like:
        continue
    try:
        importlib.import_module(mod)
    except Exception:
        missing.append(mod)

out = Path(ROOT)/'requirements_detected.txt'
with out.open('w', encoding='utf-8') as f:
    for m in missing:
        f.write(m + '\n')

print('Scanned', len(py_files), 'files')
if missing:
    print('Missing modules detected (best-effort names):')
    for m in missing:
        print(' -', m)
    print('\nWrote', out)
    sys.exit(2)
else:
    print('No missing modules detected')
    sys.exit(0)
