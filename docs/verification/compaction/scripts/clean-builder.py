#!/usr/bin/env python3
"""Clean only incremental data in the explicitly owned MiniCore build caches."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

report_root = Path('/root/minicore-compaction.j5Jaqn')
base = Path('/root/minicore-tui-027-RXdxEP')
roots = [base / name for name in ('target-linux', 'target-msrv', 'target-macos-llvm')]
protected = Path('/root/minicore-runtime-v04-build')
assert all(root.is_dir() and not root.is_symlink() for root in roots)
assert not any(root == protected or protected in root.parents for root in roots)


def active_builds():
    result = []
    names = {'cargo', 'rustc', 'rustfmt', 'clippy-driver', 'rustdoc',
             'clang', 'clang-19', 'ld.lld', 'ld64.lld', 'dsymutil'}
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            name = (proc / 'comm').read_text().strip()
            if name not in names:
                continue
            cwd = (proc / 'cwd').resolve(strict=True)
            assert not (str(cwd).startswith('/root/minicore-followups.')
                        or str(cwd).startswith('/root/minicore-compaction.')), f'owned build active: {proc.name}'
            assert not any(cwd == root or root in cwd.parents for root in roots), f'cache build active: {proc.name}'
            for descriptor in (proc / 'fd').iterdir():
                try:
                    target = Path(os.readlink(descriptor))
                except (FileNotFoundError, PermissionError, OSError):
                    continue
                assert not any(target == root or root in target.parents for root in roots), f'owned cache open: {proc.name}'
            result.append({'pid': int(proc.name), 'name': name, 'cwd': str(cwd)})
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
    return result


def retained_file(path):
    return (path.stat().st_mode & 0o111 != 0
            or path.suffix in {'.rlib', '.so', '.dylib', '.a', '.dll', '.exe'}
            or any(part.endswith('.dSYM') for part in path.parts))


def checksum(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for data in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(data)
    return digest.hexdigest()


before_active = active_builds()
removable = []
retained = {}
for root in roots:
    for directory, dirs, files in os.walk(root, followlinks=False):
        parent = Path(directory)
        dirs[:] = [name for name in dirs if not (parent / name).is_symlink()]
        if parent.name == 'incremental':
            files_below = [path for path in parent.rglob('*') if path.is_file() and not path.is_symlink()]
            assert not any(retained_file(path) for path in files_below), f'non-incremental artifact in {parent}'
            removable.append({'path': str(parent), 'logical_bytes': sum(path.stat().st_size for path in files_below)})
            dirs.clear()
            continue
        for name in files:
            path = parent / name
            if not path.is_symlink() and retained_file(path):
                retained[str(path)] = checksum(path)
report = {'roots': [str(root) for root in roots], 'protected_root': str(protected),
          'active_builds_before': before_active, 'candidates': removable,
          'retained_artifact_count': len(retained), 'free_bytes_before': shutil.disk_usage(base).free}
if '--clean' not in sys.argv:
    print(json.dumps(report, indent=2))
    raise SystemExit(0)
assert not (report_root / 'cleanup.json').exists(), 'do not overwrite previous cleanup evidence'
active_builds()
(report_root / 'cleanup-preserved-hashes.json').write_text(json.dumps(retained, indent=2) + '\n')
for entry in removable:
    path = Path(entry['path'])
    assert path.name == 'incremental' and not path.is_symlink()
    assert any(root in path.parents for root in roots)
    shutil.rmtree(path)
for name, expected in retained.items():
    assert checksum(Path(name)) == expected, f'preserved artifact changed: {name}'
report.update(retained_hashes_verified=True, logical_bytes_removed=sum(item['logical_bytes'] for item in removable),
              free_bytes_after=shutil.disk_usage(base).free, active_builds_after=active_builds())
(report_root / 'cleanup.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report, indent=2))
