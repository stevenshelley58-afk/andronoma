from pathlib import Path
import json, sys
m = json.loads(Path('spec/MANIFEST.json').read_text())
missing = [p for p in m['required_files'] if not Path(p).exists()]
if missing:
    print('Missing spec files:', ', '.join(missing)); sys.exit(1)
print('Spec manifest OK')
