# Pre-Commit Verification for Absolute Requirements

This document provides a script and checklist to verify code meets absolute requirements BEFORE committing.

## Automated Pre-Commit Check Script

Create this as `.git/hooks/pre-commit` (executable):

```bash
#!/bin/bash

# Pre-commit hook to catch assumption-based measurement code
# This prevents commits that violate the "no assumption-based code" requirement

set -e

echo "Running pre-commit verification..."

# Check 1: Search for assumption-related comments
echo "✓ Checking for assumption-based code patterns..."

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

VIOLATIONS=0

# Pattern 1: "In real scenario"
if git diff --cached --name-only | xargs grep -l "In real scenario" 2>/dev/null; then
    echo -e "${RED}✗ VIOLATION: Found 'In real scenario' comment - code is simulated${NC}"
    echo "  This indicates the code does not measure actual behavior"
    VIOLATIONS=$((VIOLATIONS + 1))
fi

# Pattern 2: "For now we"
if git diff --cached --name-only | xargs grep -l "For now we" 2>/dev/null; then
    echo -e "${RED}✗ VIOLATION: Found 'For now we' comment - code is incomplete${NC}"
    echo "  This indicates the code is using temporary/assumed values"
    VIOLATIONS=$((VIOLATIONS + 1))
fi

# Pattern 3: "We'd measure" or "we'd measure"
if git diff --cached --name-only | xargs grep -l -i "we'd measure" 2>/dev/null; then
    echo -e "${RED}✗ VIOLATION: Found 'we'd measure' comment - code is simulated${NC}"
    echo "  This indicates the code does not actually measure"
    VIOLATIONS=$((VIOLATIONS + 1))
fi

# Pattern 4: expected_bytes being used in output
if git diff --cached examples/datafusion/*.rs 2>/dev/null | grep -E "^[+].*expected_bytes.*csv|^[+].*expected_bytes.*csv" >/dev/null; then
    echo -e "${RED}✗ VIOLATION: expected_bytes being written to measurement output${NC}"
    echo "  This indicates assumptions are being presented as measurements"
    VIOLATIONS=$((VIOLATIONS + 1))
fi

if [ $VIOLATIONS -gt 0 ]; then
    echo ""
    echo -e "${RED}PRE-COMMIT VERIFICATION FAILED${NC}"
    echo "Fix the violations above before committing."
    echo ""
    echo "These are not warnings - they are absolute requirements:"
    echo "- Do NOT use predetermined values in measurement output"
    echo "- Do NOT write code with comments admitting it's not measuring"
    echo "- Do NOT present assumptions as measured results"
    exit 1
fi

echo -e "${GREEN}✓ Pre-commit verification passed${NC}"
exit 0
```

## How to Install

1. Copy the script above to `.git/hooks/pre-commit`
2. Make it executable: `chmod +x .git/hooks/pre-commit`
3. The script will run automatically before each commit

## Manual Pre-Commit Checklist

Before committing, run this checklist:

### Step 1: Search for Red Flag Patterns

```bash
# Check for all red flag patterns
echo "=== Searching for assumption-based code patterns ==="
echo ""
echo "Pattern 1: 'In real scenario'"
git diff --cached --name-only | xargs grep -n "In real scenario" 2>/dev/null || echo "  ✓ Not found"
echo ""
echo "Pattern 2: 'For now'"
git diff --cached --name-only | xargs grep -n "For now" 2>/dev/null || echo "  ✓ Not found"
echo ""
echo "Pattern 3: 'We'd measure'"
git diff --cached --name-only | xargs grep -n -i "we'd measure" 2>/dev/null || echo "  ✓ Not found"
echo ""
echo "Pattern 4: expected_* variables in output"
git diff --cached examples/datafusion/*.rs 2>/dev/null | grep "expected_" || echo "  ✓ Not found"
```

### Step 2: Verify Benchmark Code

For any benchmark files being committed:

- [ ] Does the code measure ACTUAL responses from the system?
- [ ] Are there any hardcoded values that represent "expected" behavior?
- [ ] Do comments anywhere say "would measure" or "in real scenario"?
- [ ] Is the code submitting actual filter expressions to backends?
- [ ] Is the code reading actual response sizes (Content-Length, etc.)?
- [ ] Would this code produce identical results if run again?

### Step 3: Verify Documentation

For any documentation being updated:

- [ ] Does it clearly state what is measured vs. simulated?
- [ ] Are performance numbers backed by actual measurements?
- [ ] Does it correctly describe what each backend actually does?
- [ ] Are there any claims about capabilities without proof?

## What to Do If Violations Are Found

**Option 1: Fix the Code** (RECOMMENDED)
```bash
# Rewrite benchmark code to measure actual behavior
# Instead of using expected_bytes, read from response
# Actually invoke backend APIs properly
# Re-run benchmarks to get real measurements
```

**Option 2: Mark Code as Simulation**
```rust
// If code cannot be fixed immediately, mark it clearly:
// SIMULATION - NOT MEASURED
// This benchmark uses theoretical values and does not measure actual behavior.
// TODO: Implement actual measurement by [date]
```

**Option 3: Reject the Commit**
```bash
# If neither option works:
# Do not commit until code measures reality
git reset
# Fix the code or remove it
```

## For Code Review

When reviewing code before commit, check:

1. **Measurement verification**: Can you trace actual data from system to output?
2. **Comment audit**: Do comments indicate simulation or measurement?
3. **Parameter tracking**: Are "expected_" parameters being passed to output?
4. **Backend behavior**: Is code actually testing what backend does?

If any answer suggests assumption-based code, request changes.

## The Single Rule

**NEVER commit code that presents assumptions or theoretical values as if they were measured.**

This is not negotiable. It's not a style preference. It's a requirement.

If you find yourself writing comments like "In real scenario we'd measure", STOP.
Either measure it properly or don't commit it.
