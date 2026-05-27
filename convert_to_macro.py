#!/usr/bin/env python3
import os
import re
from pathlib import Path

def convert_file(filepath):
    """Convert a response file to use impl_from_madmin_response! macro"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Check if already using macro
    if 'impl_from_madmin_response!' in content:
        return False

    # Check if it has the standard pattern
    if 'mem::take(resp.headers_mut())' not in content:
        return False

    # Find struct name
    match = re.search(r'^pub struct (\w+Response)', content, re.MULTILINE)
    if not match:
        return False

    struct_name = match.group(1)

    # Add macro import if not present
    if 'use crate::impl_from_madmin_response;' not in content:
        content = content.replace(
            'use crate::impl_has_madmin_fields;',
            'use crate::impl_from_madmin_response;\nuse crate::impl_has_madmin_fields;'
        )

    # Update imports
    content = re.sub(
        r'use crate::madmin::types::\{FromMadminResponse, MadminRequest\};',
        'use crate::madmin::types::MadminRequest;',
        content
    )
    content = re.sub(r'use async_trait::async_trait;\n', '', content)
    content = re.sub(r'use std::mem;\n', '', content)

    # Remove manual FromMadminResponse implementation (more aggressive pattern)
    # Match from #[async_trait] to the end of the impl block
    pattern = r'\n+#\[async_trait\]\s*\n*impl FromMadminResponse for ' + struct_name + r'\s*\{.*?\n\}\s*(?=\n|$)'
    content = re.sub(pattern, '', content, flags=re.DOTALL)

    # Also handle cases where async_trait is used
    pattern2 = r'\n+use async_trait::async_trait;\s*\n+#\[async_trait\]\s*\n*impl FromMadminResponse for ' + struct_name + r'\s*\{.*?\n\}\s*(?=\n|$)'
    content = re.sub(pattern2, '', content, flags=re.DOTALL)

    # Remove standalone TODO comments
    content = re.sub(r'\n\n//TODO why is this implementation not handled with\s*\n', '\n', content)

    # Add macro call before impl_has_madmin_fields if not present
    if f'impl_from_madmin_response!({struct_name});' not in content:
        content = content.replace(
            f'impl_has_madmin_fields!({struct_name});',
            f'impl_from_madmin_response!({struct_name});\nimpl_has_madmin_fields!({struct_name});'
        )

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

    return True

# Find all response files
response_dir = Path('src/madmin/response')
converted = []

for rs_file in response_dir.rglob('*.rs'):
    if convert_file(rs_file):
        converted.append(str(rs_file))
        print(f"Converted: {rs_file}")

print(f"\nTotal converted: {len(converted)}")
