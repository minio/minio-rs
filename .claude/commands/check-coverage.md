# Check Test Coverage

Analyze code coverage for the MinIO Rust SDK and provide a detailed report.

## Your Task

1. **Install cargo-llvm-cov if needed**
   - Check if llvm-cov is installed: `cargo llvm-cov --version`
   - If not installed: `cargo install cargo-llvm-cov`
   - This tool works well on Windows and all platforms

2. **Run Coverage Analysis**
   - For text report: `cargo llvm-cov --lib --tests`
   - For HTML report: `cargo llvm-cov --lib --tests --html --output-dir target/coverage`
   - For detailed output: `cargo llvm-cov --lib --tests --text`
   - Focus on library code, not test code itself

3. **Parse and Present Results**
   - Show overall coverage percentage
   - List files with their coverage percentages
   - Identify files/functions with <100% coverage
   - Highlight critical uncovered code paths in `src/madmin` and `src/s3`
   - Separate coverage by module (madmin vs s3)

4. **Provide Actionable Report**
   Present findings in this format:

   ```
   ## Coverage Summary
   - Overall: XX.XX%
   - Lines covered: XXXX / XXXX
   - Functions covered: XXX / XXX

   ### Module Breakdown
   - src/madmin: XX.XX% (XXXX/XXXX lines)
   - src/s3: XX.XX% (XXXX/XXXX lines)

   ## Files Below 100% Coverage

   ### MinIO Admin (madmin)
   #### src/madmin/builders/some_file.rs (XX.XX%)
   - Line 45-52: Error handling path not tested
   - Line 78: Builder method combination not covered

   #### src/madmin/response/other_file.rs (XX.XX%)
   - Line 23-25: JSON parsing error path missing test

   ### S3 API (s3)
   #### src/s3/client.rs (XX.XX%)
   - Line 123-130: Error handling for network failures
   - Line 245: Retry logic not tested

   #### src/s3/args/some_arg.rs (XX.XX%)
   - Line 67-70: Validation edge case

   ## Recommendations
   1. [madmin] Add test for error case in some_file.rs:45-52
   2. [madmin] Test builder method combinations in some_file.rs:78
   3. [s3] Add network failure test in client.rs:123-130
   4. [s3] Test validation edge case in args/some_arg.rs:67-70
   ```

5. **Suggest Next Steps**
   - Recommend which tests to write first (prioritize critical paths)
   - Suggest whether to run `/test-coverage` to auto-generate tests
   - Identify if any coverage gaps are in trivial code that can be ignored

Do not make any code changes - only analyze and report.
