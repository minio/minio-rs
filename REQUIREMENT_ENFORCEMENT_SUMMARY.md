# Requirement Enforcement Summary

This document summarizes the three-layer enforcement system for absolute requirements.

## The Problem

Assumption-based code was committed that:
- Used hardcoded "expected_bytes" values instead of measuring actual data
- Contained comments admitting "In real scenario we'd measure" (i.e., not measuring)
- Made it appear Garage supports pushdown filtering when it doesn't
- Generated CSV output with assumed values presented as measurements
- Wasted engineering time investigating non-existent capabilities

## The Solution: Three-Layer Enforcement

### Layer 1: CLAUDE.md (Documentation of Requirement)

**File**: `CLAUDE.md` - Lines 26-104

Added new section: "CRITICAL: No Assumption-Based Code in Benchmarks or Measurements"

**What it contains**:
- The absolute rule: No predetermined values in measurement code
- 5 specific requirements for measurement code
- Example of what went wrong (real_pushdown_benchmark.rs)
- Example of what should have been done
- Red flags to search for during code review

**Purpose**: Make the requirement explicit and non-negotiable

### Layer 2: ABSOLUTE_REQUIREMENTS_CHECKLIST.md (Enforcement Process)

**File**: `ABSOLUTE_REQUIREMENTS_CHECKLIST.md`

**What it contains**:
- 5-level verification checklist
- Before writing
- During writing
- Before committing
- Before release
- After issues are found

**Key question it enforces**:
> "If someone asks me 'Did you actually measure this?', can I say YES without qualification?"

**Purpose**: Provide structured process to catch violations

### Layer 3: PRE_COMMIT_VERIFICATION.md (Automated Prevention)

**File**: `PRE_COMMIT_VERIFICATION.md`

**What it contains**:
- Pre-commit hook script that blocks violations
- Pattern matching for red flags:
  - "In real scenario"
  - "For now"
  - "we'd measure"
  - expected_* in output
- Manual verification checklist
- Step-by-step instructions

**Purpose**: Prevent bad commits from being created

## How It Works Together

```
CODE BEING WRITTEN
    ↓
Layer 1: Developer reads CLAUDE.md requirement
    ↓ (Do I understand the rule?)
Developer writes code
    ↓
Layer 2: Developer uses ABSOLUTE_REQUIREMENTS_CHECKLIST
    ↓ (Does my code meet the requirement?)
git add [files]
git commit
    ↓
Layer 3: Pre-commit hook runs verification
    ↓ (Pattern matching for violations)
    ├─ VIOLATIONS FOUND → COMMIT BLOCKED
    │                    Prints error message
    │                    Developer must fix
    │
    └─ NO VIOLATIONS → COMMIT SUCCEEDS
```

## Key Enforcement Points

**BEFORE WRITING**: Read requirement in CLAUDE.md (Layer 1)

**DURING REVIEW**: Use ABSOLUTE_REQUIREMENTS_CHECKLIST (Layer 2)
- Specifically check for red flags
- Answer the core question
- Verify measurement method

**BEFORE COMMIT**: Pre-commit hook (Layer 3)
- Automatically searches for violations
- Blocks commit if found
- Prints clear error message

## The Red Flags (What Gets Caught)

The pre-commit hook searches for:

1. `"In real scenario"` - Indicates code is simulating
2. `"For now we"` - Indicates incomplete measurement
3. `"we'd measure"` - Indicates code doesn't measure
4. `expected_bytes` in CSV output - Indicates assumptions in results

Any of these patterns → Commit blocked

## Why This Three-Layer Approach

**One layer alone is insufficient**:
- Just CLAUDE.md → Easy to miss or forget
- Just checklist → Requires discipline, no automation
- Just pre-commit hook → May be bypassed, needs understanding

**Three layers together**:
1. Documentation makes requirement explicit
2. Checklist ensures systematic thinking
3. Pre-commit hook provides automated enforcement

## How to Use This System

### For Developers

1. **Before writing benchmark code**:
   - Read the "No Assumption-Based Code" section in CLAUDE.md
   - Ask yourself the core question: "Will I measure actual behavior?"

2. **While writing**:
   - Use ABSOLUTE_REQUIREMENTS_CHECKLIST.md as you code
   - Regularly ask: "Am I measuring or assuming?"

3. **Before committing**:
   - Pre-commit hook will catch violations
   - If blocked: Fix the code, don't bypass the hook

4. **If violations are found**:
   - Understand why the code was rejected
   - Either measure properly or mark code as SIMULATION
   - Don't commit assumption-based code

### For Code Reviewers

1. Check CLAUDE.md requirement (Lines 26-104)
2. Review code against ABSOLUTE_REQUIREMENTS_CHECKLIST
3. Specifically look for red flags mentioned in Layer 3
4. Reject code that violates the requirement

## Implementation Checklist

- [x] Add requirement to CLAUDE.md (Layer 1)
- [x] Create ABSOLUTE_REQUIREMENTS_CHECKLIST.md (Layer 2)
- [x] Create PRE_COMMIT_VERIFICATION.md with script (Layer 3)
- [ ] Install pre-commit hook: `chmod +x .git/hooks/pre-commit`
- [ ] Add to this document that hook is installed
- [ ] Communicate requirement to team
- [ ] Train team on the three layers

## To Install Pre-Commit Hook

```bash
# Navigate to repository
cd /path/to/minio-rs

# Create hooks directory if needed
mkdir -p .git/hooks

# Copy the script from PRE_COMMIT_VERIFICATION.md to .git/hooks/pre-commit
# Make it executable
chmod +x .git/hooks/pre-commit

# Test it works
git add test-file.txt
git commit -m "test"  # Should succeed if no violations
```

## The Bottom Line

**This system prevents assumption-based code from being committed by:**

1. Making the requirement explicit (CLAUDE.md)
2. Providing structured verification (Checklist)
3. Automating enforcement (Pre-commit hook)

**Combined effect**: Assumption-based code should never reach production.

**If it does**: The three layers make it easier to identify and correct.

---

**Status**: Implementation Complete

**Next Step**: Install pre-commit hook and verify it works
