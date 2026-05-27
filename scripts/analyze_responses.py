#!/usr/bin/env python3
"""
Analyze madmin response files to categorize them for lazy parsing refactoring.

This script scans all response files and categorizes them into:
1. Type aliases - no refactoring needed
2. Empty responses - just need metadata storage
3. Simple responses with data fields - need lazy getters
4. Stream responses - skip (special handling)
5. Already refactored - has impl_has_madmin_fields
"""

import os
import re
from pathlib import Path
from typing import Dict, List, Tuple

def analyze_response_file(filepath: Path) -> Dict[str, any]:
    """Analyze a single response file and categorize it."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    result = {
        'path': str(filepath),
        'category': 'unknown',
        'needs_refactoring': False,
        'fields': [],
        'struct_name': None,
    }

    # Check if it's a type alias
    if re.search(r'pub\s+type\s+\w+Response\s*=', content):
        result['category'] = 'type_alias'
        return result

    # Check if already refactored
    if 'impl_has_madmin_fields!' in content:
        result['category'] = 'already_refactored'
        return result

    # Check if it's a stream response
    if 'Stream' in content and 'Pin<Box<dyn Stream' in content:
        result['category'] = 'stream_response'
        return result

    # Extract struct name
    struct_match = re.search(r'pub\s+struct\s+(\w+Response)\s*\{', content)
    if not struct_match:
        result['category'] = 'no_struct'
        return result

    result['struct_name'] = struct_match.group(1)

    # Extract fields from struct definition
    struct_content = re.search(
        r'pub\s+struct\s+\w+Response\s*\{([^\}]+)\}',
        content,
        re.DOTALL
    )

    if struct_content:
        fields_text = struct_content.group(1)
        # Find all pub fields
        pub_fields = re.findall(r'pub\s+(\w+):\s*([^,\n]+)', fields_text)
        result['fields'] = [(name, typ.strip()) for name, typ in pub_fields]

    # Check if it has request, headers, body already
    has_request = any(name == 'request' for name, _ in result['fields'])
    has_headers = any(name == 'headers' for name, _ in result['fields'])
    has_body = any(name == 'body' for name, _ in result['fields'])

    if has_request and has_headers and has_body:
        result['category'] = 'partial_refactor'
        result['needs_refactoring'] = True
    elif len(result['fields']) == 0:
        result['category'] = 'empty_struct'
        result['needs_refactoring'] = True
    elif len(result['fields']) > 0:
        result['category'] = 'has_fields'
        result['needs_refactoring'] = True

    return result

def main():
    # Find all response files
    response_dir = Path('src/madmin/response')
    response_files = list(response_dir.rglob('*.rs'))
    response_files = [f for f in response_files if f.name not in ['mod.rs', 'response_traits.rs']]

    # Analyze all files
    results = []
    for filepath in response_files:
        result = analyze_response_file(filepath)
        results.append(result)

    # Categorize
    categories = {}
    for result in results:
        cat = result['category']
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(result)

    # Print summary
    print("# Response File Analysis\n")
    print(f"Total files analyzed: {len(results)}\n")

    for category, items in sorted(categories.items()):
        print(f"## {category.replace('_', ' ').title()}: {len(items)} files")
        if category in ['has_fields', 'empty_struct', 'partial_refactor']:
            print(f"   **Needs refactoring**")
            for item in items[:5]:  # Show first 5
                print(f"   - {Path(item['path']).name}: {item['struct_name']}")
                if item['fields']:
                    for fname, ftype in item['fields'][:3]:
                        print(f"      • {fname}: {ftype}")
            if len(items) > 5:
                print(f"   ... and {len(items) - 5} more")
        print()

    # Summary counts
    needs_refactor = sum(1 for r in results if r['needs_refactoring'])
    print(f"\n**Summary:**")
    print(f"- Already refactored: {len(categories.get('already_refactored', []))}")
    print(f"- Type aliases (skip): {len(categories.get('type_alias', []))}")
    print(f"- Stream responses (skip): {len(categories.get('stream_response', []))}")
    print(f"- **Need refactoring: {needs_refactor}**")

if __name__ == '__main__':
    main()
