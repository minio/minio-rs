#!/usr/bin/env python3
"""
Comprehensive test fixing script for lazy parsing integration
"""
import re
import sys
from pathlib import Path

def fix_file(filepath):
    """Fix a single file"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    original = content

    # Pattern 1: Result types that need unwrap (like log_config.status())
    # Match: let status = log_config.status();
    # Replace: let status = log_config.status().expect("...");
    content = re.sub(
        r'(\w+)\.status\(\);',
        r'\1.status().expect("Failed to parse status");',
        content
    )

    # Pattern 2: Response getters that return Result<T>
    # Match patterns like resp.info.field or resp.status.field
    patterns = [
        (r'let (\w+) = (\w+)\.info\(\);', r'let \1 = \2.info().expect("Failed to parse info");'),
        (r'let (\w+) = (\w+)\.status\(\);', r'let \1 = \2.status().expect("Failed to parse status");'),
        (r'let (\w+) = (\w+)\.result\(\);', r'let \1 = \2.result().expect("Failed to parse result");'),
        (r'let (\w+) = (\w+)\.action\(\);', r'let \1 = \2.action().expect("Failed to parse action");'),
        (r'let (\w+) = (\w+)\.sites\(\);', r'let \1 = \2.sites().expect("Failed to parse sites");'),
    ]

    for pattern, replacement in patterns:
        content = re.sub(pattern, replacement, content)

    # Pattern 3: Direct field access on responses in expressions
    # Match: if resp.field { ... } or resp.field.something
    field_patterns = [
        (r'(\w+_resp)\.action([,\s\.;\)])', r'\1.action().unwrap()\2'),
        (r'(\w+_resp)\.success([,\s\.;\)])', r'\1.success().unwrap()\2'),
        (r'(\w+_resp)\.stopped_at([,\s\.;\)])', r'\1.stopped_at().unwrap()\2'),
        (r'(\w+_resp)\.sites([,\s\.;\)])', r'\1.sites().unwrap()\2'),
        (r'(\w+_resp)\.policy_name([,\s\.;\)])', r'\1.policy_name().unwrap()\2'),
    ]

    for pattern, replacement in field_patterns:
        content = re.sub(pattern, replacement, content)

    # Pattern 4: UserInfoResponse and similar that have .status field
    content = re.sub(
        r'(\w+_info)\.status([,\s\.;\)])',
        r'\1.status().unwrap()\2',
        content
    )

    if content != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

def main():
    test_dirs = [
        Path('tests/madmin'),
        Path('tests'),
        Path('examples'),
    ]

    fixed_files = []

    for test_dir in test_dirs:
        if not test_dir.exists():
            continue

        for test_file in test_dir.glob('*.rs'):
            if fix_file(test_file):
                fixed_files.append(str(test_file))
                print(f"Fixed: {test_file}")

    print(f"\nTotal files fixed: {len(fixed_files)}")
    return 0

if __name__ == '__main__':
    sys.exit(main())
