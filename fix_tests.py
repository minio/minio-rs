#!/usr/bin/env python3
"""
Fix integration tests to work with lazy parsing.

Common patterns to fix:
1. resp.field -> resp.field()
2. resp.method -> resp.method() (when method returns Result)
3. Direct field access -> getter method + unwrap
"""
import re
import sys
from pathlib import Path

def fix_test_file(filepath):
    """Fix a test file to work with lazy parsing"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    original = content

    # Pattern 1: Various response field accesses that need getter methods
    patterns = [
        # Common response getters
        (r'(\w+)\.account\.', r'\1.account().unwrap().'),
        (r'(\w+_resp)\.info\.', r'\1.info().unwrap().'),
        (r'(\w+_resp)\.status\.', r'\1.status().unwrap().'),
        (r'(\w+_resp)\.result\.', r'\1.result().unwrap().'),
        (r'(\w+)\.metrics\.', r'\1.metrics().unwrap().'),
        (r'(\w+)\.users\.', r'\1.users().unwrap().'),
        (r'(\w+)\.locks\.', r'\1.locks().unwrap().'),
        (r'(\w+)\.policies\.', r'\1.policies().unwrap().'),
        (r'(\w+)\.user_info\.', r'\1.user_info().unwrap().'),

        # Response field access - these need to become method calls
        (r'(\w+_resp)\.action([,\s\)])', r'\1.action().unwrap()\2'),
        (r'(\w+_resp)\.success([,\s\)])', r'\1.success().unwrap()\2'),
        (r'(\w+_resp)\.stopped_at([,\s\)])', r'\1.stopped_at().unwrap()\2'),
        (r'(\w+_resp)\.sites([,\s\)])', r'\1.sites().unwrap()\2'),
        (r'(\w+_resp)\.policy_name([,\s\)])', r'\1.policy_name().unwrap()\2'),

        # Method calls that look like field access
        (r'(\w+_resp)\.arn([,\s\)])', r'\1.arn().unwrap()\2'),
        (r'(\w+)\.restart_required([,\s\)])', r'\1.restart_required()\2'),

        # List-like responses that need unwrap
        (r'(\w+_resp)\.tiers\(\)\.len\(\)', r'\1.tiers().unwrap().len()'),
        (r'(\w+_resp)\.tiers\(\)\.is_empty\(\)', r'\1.tiers().unwrap().is_empty()'),
        (r'(\w+_resp)\.tiers\(\)\.iter\(\)', r'\1.tiers().unwrap().iter()'),
        (r'(\w+_resp)\.tiers\(\)\[', r'\1.tiers().unwrap()['),
        (r'(\w+_resp)\.pools\(\)\.len\(\)', r'\1.pools().unwrap().len()'),
        (r'(\w+_resp)\.pools\(\)\.is_empty\(\)', r'\1.pools().unwrap().is_empty()'),
        (r'(\w+_resp)\.pools\(\)\.iter\(\)', r'\1.pools().unwrap().iter()'),
        (r'(\w+_resp)\.pools\(\)\[', r'\1.pools().unwrap()['),
        (r'(\w+_resp)\.stats\(\)\.len\(\)', r'\1.stats().unwrap().len()'),
        (r'(\w+_resp)\.stats\(\)\.iter\(\)', r'\1.stats().unwrap().iter()'),

        # Iteration patterns
        (r'for\s+(\w+)\s+in\s+&(\w+_resp)\.tiers\(\)', r'for \1 in &\2.tiers().unwrap()'),
        (r'for\s+(\w+)\s+in\s+&(\w+_resp)\.pools\(\)', r'for \1 in &\2.pools().unwrap()'),
        (r'for\s+(\w+)\s+in\s+&(\w+_resp)\.stats\(\)', r'for \1 in &\2.stats().unwrap()'),
    ]

    for pattern, replacement in patterns:
        content = re.sub(pattern, replacement, content)

    # Fix .is_empty() on responses that need .body()
    content = re.sub(
        r'(\w+_resp)\.is_empty\(\)',
        r'\1.body().is_empty()',
        content
    )

    if content != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

def main():
    test_dir = Path('tests/madmin')
    fixed_files = []

    for test_file in test_dir.glob('*.rs'):
        if fix_test_file(test_file):
            fixed_files.append(str(test_file))
            print(f"Fixed: {test_file}")

    # Also fix tests in tests/ root
    for test_file in Path('tests').glob('test_*.rs'):
        if fix_test_file(test_file):
            fixed_files.append(str(test_file))
            print(f"Fixed: {test_file}")

    print(f"\nTotal files fixed: {len(fixed_files)}")
    return 0

if __name__ == '__main__':
    sys.exit(main())
