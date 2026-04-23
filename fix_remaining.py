#!/usr/bin/env python3
"""
Fix remaining integration test errors after lazy parsing
"""
import re
from pathlib import Path

def fix_file(filepath):
    """Fix a file comprehensively"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    original = content

    # Fix direct field access on responses that need method calls
    # Pattern: resp.field -> resp.field()
    field_patterns = [
        # ServiceActionResponse
        (r'(\w+)\.action([,\s\.\[\];\)])', r'\1.action().unwrap()\2'),

        # UserInfo/AddUser/RemoveUser responses
        (r'(\w+_resp)\.success([,\s\.\[\];\)])', r'\1.success().unwrap()\2'),
        (r'(\w+_info)\.status([,\s\.\[\];\)])', r'\1.status().unwrap()\2'),

        # RebalanceStatusResponse
        (r'(\w+_resp)\.stopped_at([,\s\.\[\];\)])', r'\1.stopped_at().unwrap()\2'),

        # SiteReplicationStatusResponse
        (r'(\w+_resp)\.sites([,\s\.\[\];\)])', r'\1.sites().unwrap()\2'),

        # InfoCannedPolicyResponse
        (r'(\w+_resp)\.policy_name([,\s\.\[\];\)])', r'\1.policy_name().unwrap()\2'),
    ]

    for pattern, replacement in field_patterns:
        content = re.sub(pattern, replacement, content)

    # Fix method calls that should be unwrapped
    # Pattern: let x = resp.method(); -> let x = resp.method().unwrap();
    # But only if it's not already unwrapped
    content = re.sub(
        r'let (\w+) = (\w+)\.info\(\);$',
        r'let \1 = \2.info().expect("Failed to parse info");',
        content,
        flags=re.MULTILINE
    )
    content = re.sub(
        r'let (\w+) = (\w+)\.status\(\);$',
        r'let \1 = \2.status().expect("Failed to parse status");',
        content,
        flags=re.MULTILINE
    )
    content = re.sub(
        r'let (\w+) = (\w+)\.result\(\);$',
        r'let \1 = \2.result().expect("Failed to parse result");',
        content,
        flags=re.MULTILINE
    )

    if content != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

def main():
    dirs = [
        Path('tests/madmin'),
        Path('tests'),
        Path('examples'),
    ]

    fixed = []
    for d in dirs:
        if not d.exists():
            continue
        for f in d.glob('*.rs'):
            if fix_file(f):
                fixed.append(str(f))
                print(f"Fixed: {f}")

    print(f"\nTotal: {len(fixed)}")

if __name__ == '__main__':
    main()
