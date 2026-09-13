#!/usr/bin/env python3
"""Generate a byte-exact manifest for one owned evidence directory."""
import hashlib
from pathlib import Path
import sys

root = Path(sys.argv[1]).resolve()
lines = []
for path in sorted(root.rglob('*')):
    if path.is_symlink():
        raise ValueError(f'symlink is not an archived evidence file: {path}')
    if not path.is_file() or path == root / 'FILES.sha256':
        continue
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    lines.append(f'{digest.hexdigest()}  {path.relative_to(root).as_posix()}\n')
(root / 'FILES.sha256').write_text(''.join(lines))
print(f'CHECKSUMS files={len(lines)} root={root}')
