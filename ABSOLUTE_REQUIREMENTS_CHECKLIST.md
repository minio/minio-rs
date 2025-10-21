# Absolute Requirements Checklist

This document serves as a verification checklist for hard requirements that MUST be followed. Violations are unacceptable.

## Level 1: Code Review Checkpoints (Before Writing)

When tasked with writing benchmark, measurement, or comparison code:

- [ ] **Ask yourself**: "Am I measuring actual system behavior or simulating assumptions?"
- [ ] **Ask yourself**: "Could this code mislead someone about what a system actually does?"
- [ ] **Ask yourself**: "If I can't measure it right now, should this code exist at all?"

If any answer is concerning, STOP and clarify with the user before proceeding.

## Level 2: Code Red Flags (During Writing)

Immediately REJECT code that contains:

- [ ] Comments containing "In real scenario" or "For now we use"
- [ ] Comments containing "We'd measure" or "would call"
- [ ] Variables named `expected_*`, `assumed_*`, or `hardcoded_*`
- [ ] Parameters like `expected_bytes` being used in measurement output
- [ ] Hardcoded values passed through to CSV/results as "measured"
- [ ] Simulated responses instead of actual HTTP responses
- [ ] Predetermined result values instead of measuring from real operations

## Level 3: Commit-Time Verification (Before Committing)

Before any commit, search the code for these patterns:

```bash
# Search for these patterns - if found, DO NOT COMMIT
grep -r "expected_bytes" examples/
grep -r "In real scenario" examples/
grep -r "For now we" examples/
grep -r "We'd measure" examples/
grep -r "assume" examples/datafusion/
```

If any matches are found:
1. DO NOT COMMIT
2. Rewrite the code to measure actual behavior
3. Or explicitly label it as "SIMULATION - NOT MEASURED"

## Level 4: Documentation Verification (Before Release)

- [ ] Benchmark documentation clearly states what is MEASURED vs SIMULATED
- [ ] CSV output only contains data that was actually collected
- [ ] Comments do not claim measured results for simulated data
- [ ] Changelog notes if switching from simulation to real measurement
- [ ] README documents any known limitations in measurement

## Level 5: User Communication (After Discovery of Issues)

If assumption-based code is discovered:

- [ ] Immediately notify user that results were simulated
- [ ] Identify specifically which measurements were assumed vs measured
- [ ] Provide corrected measurements if available
- [ ] Update all documentation to reflect reality
- [ ] Create issue for fixing the code to measure properly

## How to Apply This Checklist

### Example: Benchmark Code Review

**SCENARIO**: Code contains this:
```rust
// In real scenario, we'd measure actual bytes from plan_table_scan response
// For now, we use expected values
let bytes_transferred = (expected_bytes * 1024.0 * 1024.0) as u64;
```

**CHECKLIST APPLICATION**:
- [ ] Level 1: FAILED - This IS simulating, not measuring
- [ ] Level 2: FAILED - Contains "In real scenario" and "For now"
- [ ] **ACTION**: Rewrite to measure actual response

**CORRECTED CODE**:
```rust
// Actually measure what was transferred
let response = client.get_object(bucket, object).await?;
let actual_bytes = response.content_length()
    .ok_or("Cannot determine transfer size")?;
// Now this is MEASURED
```

### Example: Documentation Review

**SCENARIO**: Documentation states:
> "Both backends achieve 97% data reduction with pushdown filtering"

**CHECKLIST APPLICATION**:
- [ ] Level 4: FAILED - Is this measured or assumed?
- [ ] Check: Did we actually submit filter expressions to Garage?
- [ ] Check: Did we verify Garage returned filtered vs full data?
- [ ] If NO: Update documentation to be truthful

**CORRECTED DOCUMENTATION**:
> "MinIO achieves 97% data reduction via plan_table_scan() API.
> Garage behavior with filters was not tested in this benchmark."

## The Core Question

**Before committing ANY benchmark or measurement code, answer this:**

> "If someone asks me 'Did you actually measure this?', can I say YES without qualification?"

If the answer is NO or MAYBE, the code is not ready to commit.

## Accountability

These requirements exist because:
1. **Data integrity** - Measurements must reflect reality
2. **User trust** - Users rely on benchmarks to make decisions
3. **Engineering quality** - Wasted effort on phantom capabilities
4. **Professional responsibility** - We don't misrepresent what systems do

Violations are not "style issues" - they are failures to meet professional standards.

## Enforcement

- Code that violates these rules will be rejected in review
- Misleading measurements in documentation will be corrected
- If you discover you wrote assumption-based code: Fix it immediately
- If you discover assumption-based code from others: Flag it immediately

There are no exceptions to these requirements.
