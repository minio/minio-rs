# MinIO Rust SDK - API to Test File Cross-Reference Matrix

**Last Updated:** 2025-11-04

This document provides a detailed mapping of which test files exercise which APIs, including tests that cover multiple functions.

---

## Quick Reference: API Coverage Map

| API Function | Test Files | Coverage Type | Test Count |
|--------------|------------|---------------|------------|
| **AddUser** | `test_user_management.rs` | Direct + Lifecycle | 4 tests |
| **RemoveUser** | `test_user_management.rs` | Direct + Lifecycle | 3 tests |
| **ListUsers** | `test_user_management.rs` | Direct + Lifecycle | 5 tests |
| **GetUserInfo** | `test_user_management.rs` | Direct + Lifecycle | 3 tests |
| **SetUserStatus** | `test_user_management.rs` | Direct + Lifecycle | 4 tests |
| **AddServiceAccount** | `test_service_accounts.rs` | Direct + Lifecycle | 4 tests |
| **DeleteServiceAccount** | `test_service_accounts.rs` | Lifecycle | 2 tests |
| **ListServiceAccounts** | `test_service_accounts.rs` | Direct + Lifecycle | 3 tests |
| **InfoServiceAccount** | `test_service_accounts.rs` | Lifecycle | 2 tests |
| **UpdateServiceAccount** | `test_service_accounts.rs` | Direct + Lifecycle | 2 tests |
| **AddCannedPolicy** | `test_policy_management.rs` | Lifecycle | 2 tests |
| **RemoveCannedPolicy** | `test_policy_management.rs` | Lifecycle | 2 tests |
| **ListCannedPolicies** | `test_policy_management.rs` | Direct + Lifecycle | 2 tests |
| **InfoCannedPolicy** | `test_policy_management.rs` | Lifecycle | 1 test |
| **AttachPolicy** | `test_policy_management.rs` | Direct + Lifecycle | 2 tests |
| **DetachPolicy** | `test_policy_management.rs` | Direct + Lifecycle | 2 tests |
| **GetConfig** | `test_config_management.rs` | Direct + Lifecycle | 2 tests |
| **SetConfig** | `test_config_management.rs` | Direct + Lifecycle | 2 tests |
| **GetConfigKV** | `test_config_management.rs` | Direct + Lifecycle | 2 tests |
| **SetConfigKV** | `test_config_management.rs` | Lifecycle | 1 test |
| **DelConfigKV** | `test_config_management.rs` | Lifecycle | 1 test |
| **GetBucketQuota** | `test_quota_management.rs` | Direct | 1 test |
| **SetBucketQuota** | `test_quota_management.rs` | Direct | 1 test |
| **ListGroups** | `test_group_management.rs` | Direct + Lifecycle | 2 tests |
| **GetGroupDescription** | `test_group_management.rs` | Lifecycle | 1 test |
| **UpdateGroupMembers** | `test_group_management.rs` | Lifecycle | 1 test |
| **SetGroupStatus** | `test_group_management.rs` | Lifecycle | 1 test |
| **ListRemoteTargets** | `test_remote_targets.rs` | Direct | 4 tests |
| **SetRemoteTarget** | `test_remote_targets.rs` | Lifecycle (ignored) | 1 test |
| **UpdateRemoteTarget** | `test_remote_targets.rs` | Direct + Lifecycle (1 ignored) | 2 tests |
| **RemoveRemoteTarget** | `test_remote_targets.rs` | Direct + Lifecycle (1 ignored) | 3 tests |
| **ServerInfo** | `test_server_info.rs` | Direct | 2 tests |
| **StorageInfo** | `test_server_info.rs` | Direct (ignored) | 1 test |
| **DataUsageInfo** | `test_data_usage_info.rs` | Direct | 2 tests |
| **ServerHealthInfo** | `test_server_health_info.rs` | Direct (all ignored) | 3 tests |
| **BucketScanInfo** | `test_bucket_scan_info.rs` | Direct (1 ignored) | 2 tests |
| **ClusterAPIStats** | `test_cluster_api_stats.rs` | Direct (ignored) | 1 test |
| **Heal** | `test_heal.rs` | Direct (3 ignored) | 4 tests |
| **BackgroundHealStatus** | `test_heal.rs` | Direct (ignored) | 1 test |
| **ServiceRestart** | `test_service_restart.rs` | Direct (1 ignored) | 2 tests |

---

## Detailed Test File Analysis

### tests/madmin/test_user_management.rs (10 tests)

**APIs Used:**
- ✅ `AddUser` (create operations)
- ✅ `RemoveUser` (cleanup operations)
- ✅ `ListUsers` (verification operations)
- ✅ `GetUserInfo` (verification operations)
- ✅ `SetUserStatus` (modification operations)

**Test Breakdown:**

| Test Name | APIs Exercised | Type | Purpose |
|-----------|----------------|------|---------|
| `test_add_user` | AddUser, ListUsers | Direct | Create user and verify in list |
| `test_remove_user` | AddUser, RemoveUser, ListUsers | Direct | Create, delete, verify removal |
| `test_list_users` | ListUsers | Direct | List all users |
| `test_user_info` | AddUser, GetUserInfo, RemoveUser | Direct | Create, get info, cleanup |
| `test_user_info_nonexistent_user` | GetUserInfo | Error Case | Verify error handling |
| `test_set_user_status` | AddUser, SetUserStatus, GetUserInfo, RemoveUser | Direct | Create, disable, verify, cleanup |
| `test_set_user_status_nonexistent` | SetUserStatus | Error Case | Verify error handling |
| `test_add_user_invalid_credentials` | AddUser | Error Case | Verify validation |
| `test_remove_nonexistent_user` | RemoveUser | Error Case | Verify error handling |
| `test_add_duplicate_user` | AddUser (2x), RemoveUser | Error Case | Create, duplicate attempt, cleanup |
| `test_user_lifecycle` | AddUser, ListUsers, GetUserInfo, SetUserStatus, RemoveUser | Lifecycle | Full CRUD cycle |

**Coverage Analysis:**
- **AddUser:** Used in 7/10 tests (70%)
- **RemoveUser:** Used in 6/10 tests (60%)
- **ListUsers:** Used in 4/10 tests (40%)
- **GetUserInfo:** Used in 4/10 tests (40%)
- **SetUserStatus:** Used in 3/10 tests (30%)

---

### tests/madmin/test_service_accounts.rs (5 tests)

**APIs Used:**
- ✅ `AddServiceAccount` (create operations)
- ✅ `DeleteServiceAccount` (cleanup operations)
- ✅ `ListServiceAccounts` (verification operations)
- ✅ `InfoServiceAccount` (verification operations)
- ✅ `UpdateServiceAccount` (modification operations)

**Test Breakdown:**

| Test Name | APIs Exercised | Type | Purpose |
|-----------|----------------|------|---------|
| `test_add_service_account` | AddServiceAccount, ListServiceAccounts, DeleteServiceAccount | Direct | Create, verify, cleanup |
| `test_list_service_accounts` | AddServiceAccount, ListServiceAccounts, DeleteServiceAccount | Direct | Create, list, cleanup |
| `test_service_account_validation` | ListServiceAccounts | Direct | List validation |
| `test_service_account_with_custom_credentials` | AddServiceAccount, InfoServiceAccount, DeleteServiceAccount | Direct | Create with creds, verify, cleanup |
| `test_service_account_full_lifecycle` | AddServiceAccount, InfoServiceAccount, UpdateServiceAccount, ListServiceAccounts, DeleteServiceAccount | Lifecycle | Full CRUD cycle |

**Coverage Analysis:**
- **AddServiceAccount:** Used in 4/5 tests (80%)
- **DeleteServiceAccount:** Used in 4/5 tests (80%)
- **ListServiceAccounts:** Used in 3/5 tests (60%)
- **InfoServiceAccount:** Used in 2/5 tests (40%)
- **UpdateServiceAccount:** Used in 1/5 tests (20%)

---

### tests/madmin/test_policy_management.rs (3 tests)

**APIs Used:**
- ✅ `AddCannedPolicy` (create operations)
- ✅ `RemoveCannedPolicy` (cleanup operations)
- ✅ `ListCannedPolicies` (verification operations)
- ✅ `InfoCannedPolicy` (verification operations)
- ✅ `AttachPolicy` (user operations)
- ✅ `DetachPolicy` (user operations)

**Test Breakdown:**

| Test Name | APIs Exercised | Type | Purpose |
|-----------|----------------|------|---------|
| `test_list_canned_policies` | ListCannedPolicies | Direct | List all policies |
| `test_policy_lifecycle` | AddCannedPolicy, ListCannedPolicies, InfoCannedPolicy, RemoveCannedPolicy | Lifecycle | Create, verify, delete |
| `test_attach_detach_policy` | AddUser, AddCannedPolicy, AttachPolicy, DetachPolicy, RemoveCannedPolicy, RemoveUser | Lifecycle | Create policy, attach to user, detach, cleanup |

**Coverage Analysis:**
- **AddCannedPolicy:** Used in 2/3 tests (67%)
- **RemoveCannedPolicy:** Used in 2/3 tests (67%)
- **ListCannedPolicies:** Used in 2/3 tests (67%)
- **InfoCannedPolicy:** Used in 1/3 tests (33%)
- **AttachPolicy:** Used in 1/3 tests (33%)
- **DetachPolicy:** Used in 1/3 tests (33%)

**Note:** `test_attach_detach_policy` also uses User Management APIs (AddUser, RemoveUser)

---

### tests/madmin/test_config_management.rs (4 tests)

**APIs Used:**
- ✅ `GetConfig` (read operations)
- ✅ `SetConfig` (write operations)
- ✅ `GetConfigKV` (read KV operations)
- ✅ `SetConfigKV` (write KV operations)
- ✅ `DelConfigKV` (delete KV operations)

**Test Breakdown:**

| Test Name | APIs Exercised | Type | Purpose |
|-----------|----------------|------|---------|
| `test_get_config` | GetConfig | Direct | Get full config |
| `test_set_config` | GetConfig, SetConfig | Direct | Get, modify, set |
| `test_get_set_config_kv` | SetConfigKV, GetConfigKV, DelConfigKV | Lifecycle | Set KV, get KV, delete KV |
| `test_del_config_kv` | SetConfigKV, DelConfigKV | Direct | Set KV, delete KV |

**Coverage Analysis:**
- **GetConfig:** Used in 2/4 tests (50%)
- **SetConfig:** Used in 1/4 tests (25%)
- **GetConfigKV:** Used in 1/4 tests (25%)
- **SetConfigKV:** Used in 2/4 tests (50%)
- **DelConfigKV:** Used in 2/4 tests (50%)

---

### tests/madmin/test_quota_management.rs (2 tests)

**APIs Used:**
- ✅ `GetBucketQuota` (read operations)
- ✅ `SetBucketQuota` (write operations)

**Test Breakdown:**

| Test Name | APIs Exercised | Type | Purpose |
|-----------|----------------|------|---------|
| `test_get_bucket_quota` | GetBucketQuota | Direct | Get quota settings |
| `test_set_bucket_quota` | SetBucketQuota | Direct | Set quota limits |

**Coverage Analysis:**
- **GetBucketQuota:** Used in 1/2 tests (50%)
- **SetBucketQuota:** Used in 1/2 tests (50%)

**Note:** Tests are independent, not lifecycle tests

---

### tests/madmin/test_group_management.rs (2 tests)

**APIs Used:**
- ✅ `ListGroups` (list operations)
- ✅ `GetGroupDescription` (read operations)
- ✅ `UpdateGroupMembers` (modification operations)
- ✅ `SetGroupStatus` (modification operations)

**Test Breakdown:**

| Test Name | APIs Exercised | Type | Purpose |
|-----------|----------------|------|---------|
| `test_list_groups` | ListGroups | Direct | List all groups |
| `test_group_lifecycle` | AddUser, UpdateGroupMembers, ListGroups, GetGroupDescription, SetGroupStatus, RemoveUser | Lifecycle | Create user, add to group, verify, disable group, cleanup |

**Coverage Analysis:**
- **ListGroups:** Used in 2/2 tests (100%)
- **GetGroupDescription:** Used in 1/2 tests (50%)
- **UpdateGroupMembers:** Used in 1/2 tests (50%)
- **SetGroupStatus:** Used in 1/2 tests (50%)

**Note:** `test_group_lifecycle` also uses User Management APIs (AddUser, RemoveUser)

---

### tests/madmin/test_remote_targets.rs (9 tests)

**APIs Used:**
- ✅ `ListRemoteTargets` (list operations)
- ✅ `SetRemoteTarget` (create operations)
- ✅ `UpdateRemoteTarget` (modification operations)
- ✅ `RemoveRemoteTarget` (delete operations)

**Test Breakdown:**

| Test Name | APIs Exercised | Type | Status |
|-----------|----------------|------|--------|
| `test_list_remote_targets` | ListRemoteTargets | Direct | ✅ PASSING |
| `test_list_remote_targets_invalid_bucket` | ListRemoteTargets | Error Case | ✅ PASSING |
| `test_list_remote_targets_nonexistent_bucket` | ListRemoteTargets | Error Case | ✅ PASSING |
| `test_set_remote_target` | SetRemoteTarget, ListRemoteTargets, RemoveRemoteTarget | Lifecycle | 🔴 IGNORED (multi-instance) |
| `test_update_remote_target` | SetRemoteTarget, UpdateRemoteTarget, RemoveRemoteTarget | Lifecycle | 🔴 IGNORED (multi-instance) |
| `test_update_remote_target_missing_arn` | UpdateRemoteTarget | Error Case | ✅ PASSING |
| `test_remove_remote_target` | SetRemoteTarget, RemoveRemoteTarget | Direct | 🔴 IGNORED (multi-instance) |
| `test_remove_remote_target_empty_arn` | RemoveRemoteTarget | Error Case | ✅ PASSING |
| `test_remove_remote_target_invalid_bucket` | RemoveRemoteTarget | Error Case | ✅ PASSING |

**Coverage Analysis:**
- **ListRemoteTargets:** Used in 4/9 tests (44%)
- **SetRemoteTarget:** Used in 3/9 tests (33%) - all ignored
- **UpdateRemoteTarget:** Used in 2/9 tests (22%) - 1 passing, 1 ignored
- **RemoveRemoteTarget:** Used in 5/9 tests (56%) - 3 passing, 1 ignored

**Note:** 3 tests require multi-instance MinIO setup (source + target servers)

---

### tests/madmin/test_server_info.rs (3 tests)

**APIs Used:**
- ✅ `ServerInfo` (server status operations)
- ✅ `StorageInfo` (storage metrics operations)

**Test Breakdown:**

| Test Name | APIs Exercised | Type | Status |
|-----------|----------------|------|--------|
| `test_server_info` | ServerInfo | Direct | ✅ PASSING |
| `test_server_info_with_drive_details` | ServerInfo | Direct | ✅ PASSING |
| `test_storage_info` | StorageInfo | Direct | 🔴 IGNORED (struct verification needed) |

**Coverage Analysis:**
- **ServerInfo:** Used in 2/3 tests (67%)
- **StorageInfo:** Used in 1/3 tests (33%) - ignored

---

### tests/madmin/test_data_usage_info.rs (2 tests)

**APIs Used:**
- ✅ `DataUsageInfo` (usage statistics operations)

**Test Breakdown:**

| Test Name | APIs Exercised | Type | Purpose |
|-----------|----------------|------|---------|
| `test_data_usage_info` | DataUsageInfo | Direct | Get usage info with capacity |
| `test_data_usage_info_without_capacity` | DataUsageInfo | Direct | Get usage info without capacity flag |

**Coverage Analysis:**
- **DataUsageInfo:** Used in 2/2 tests (100%)

---

### tests/madmin/test_service_restart.rs (2 tests)

**APIs Used:**
- ✅ `ServiceRestart` (service control operations)

**Test Breakdown:**

| Test Name | APIs Exercised | Type | Status |
|-----------|----------------|------|--------|
| `test_service_restart` | ServiceRestart | Direct | 🔴 IGNORED (would restart server) |
| `test_service_restart_unauthorized` | ServiceRestart | Error Case | ✅ PASSING |

**Coverage Analysis:**
- **ServiceRestart:** Used in 2/2 tests (100%)

**Note:** Actual restart test intentionally ignored to prevent disruption

---

### tests/madmin/test_server_health_info.rs (3 tests)

**APIs Used:**
- ✅ `ServerHealthInfo` (health monitoring operations)

**Test Breakdown:**

| Test Name | APIs Exercised | Type | Status |
|-----------|----------------|------|--------|
| `test_server_health_info_basic` | ServerHealthInfo | Direct | 🔴 IGNORED (JSON parsing issue) |
| `test_server_health_info_all_checks` | ServerHealthInfo | Direct | 🔴 IGNORED (JSON parsing issue) |
| `test_server_health_info_selective_checks` | ServerHealthInfo | Direct | 🔴 IGNORED (JSON parsing issue) |

**Coverage Analysis:**
- **ServerHealthInfo:** Used in 3/3 tests (100%)

**Note:** Tests require fixing response parser to handle newline-delimited JSON or streaming format

---

### tests/madmin/test_bucket_scan_info.rs (2 tests)

**APIs Used:**
- ✅ `BucketScanInfo` (bucket scanning operations)

**Test Breakdown:**

| Test Name | APIs Exercised | Type | Status |
|-----------|----------------|------|--------|
| `test_bucket_scan_info` | BucketScanInfo | Direct | 🔴 IGNORED (newly created buckets have no scan status) |
| `test_bucket_scan_info_invalid_bucket` | BucketScanInfo | Error Case | ✅ PASSING |

**Coverage Analysis:**
- **BucketScanInfo:** Used in 2/2 tests (100%)

**Note:** One test requires using existing bucket with scan history or waiting for scan to complete

---

### tests/madmin/test_cluster_api_stats.rs (1 test)

**APIs Used:**
- ✅ `ClusterAPIStats` (cluster metrics operations)

**Test Breakdown:**

| Test Name | APIs Exercised | Type | Status |
|-----------|----------------|------|--------|
| `test_cluster_api_stats` | ClusterAPIStats | Direct | 🔴 IGNORED (struct definition needs refinement) |

**Coverage Analysis:**
- **ClusterAPIStats:** Used in 1/1 tests (100%)

**Note:** Struct definition needs adjustment to match actual API response format

---

### tests/madmin/test_heal.rs (4 tests)

**APIs Used:**
- ✅ `Heal` (healing operations)
- ✅ `BackgroundHealStatus` (healing status operations)

**Test Breakdown:**

| Test Name | APIs Exercised | Type | Status |
|-----------|----------------|------|--------|
| `test_heal_start_dry_run` | Heal | Direct | 🔴 IGNORED (requires distributed MinIO) |
| `test_heal_with_prefix` | Heal | Direct | 🔴 IGNORED (requires distributed MinIO) |
| `test_heal_invalid_bucket` | Heal | Error Case | ✅ PASSING |
| `test_background_heal_status` | BackgroundHealStatus | Direct | 🔴 IGNORED (requires distributed MinIO) |

**Coverage Analysis:**
- **Heal:** Used in 3/4 tests (75%)
- **BackgroundHealStatus:** Used in 1/4 tests (25%)

**Note:** Heal APIs require distributed MinIO deployment (not available in single-node 'xl-single' mode)

---

## Multi-API Test Analysis

These tests exercise **3 or more APIs** in a single test:

### Complex Lifecycle Tests

| Test Name | File | API Count | APIs Used |
|-----------|------|-----------|-----------|
| `test_user_lifecycle` | `test_user_management.rs` | 5 | AddUser, ListUsers, GetUserInfo, SetUserStatus, RemoveUser |
| `test_service_account_full_lifecycle` | `test_service_accounts.rs` | 5 | AddServiceAccount, InfoServiceAccount, UpdateServiceAccount, ListServiceAccounts, DeleteServiceAccount |
| `test_attach_detach_policy` | `test_policy_management.rs` | 6 | AddUser, AddCannedPolicy, AttachPolicy, DetachPolicy, RemoveCannedPolicy, RemoveUser |
| `test_group_lifecycle` | `test_group_management.rs` | 6 | AddUser, UpdateGroupMembers, ListGroups, GetGroupDescription, SetGroupStatus, RemoveUser |
| `test_get_set_config_kv` | `test_config_management.rs` | 3 | SetConfigKV, GetConfigKV, DelConfigKV |

### Cross-Category Tests

Tests that use APIs from **multiple categories**:

| Test Name | File | Categories Used |
|-----------|------|-----------------|
| `test_attach_detach_policy` | `test_policy_management.rs` | User Management + Policy Management |
| `test_group_lifecycle` | `test_group_management.rs` | User Management + Group Management |

---

## Coverage Statistics by Test Type

### Direct Tests (Single API Focus)
- **Count:** 20 tests
- **Purpose:** Test specific API functionality
- **Examples:** `test_list_users`, `test_get_bucket_quota`, `test_server_info`

### Lifecycle Tests (Multi-API)
- **Count:** 11 tests
- **Purpose:** Test complete workflows (create → verify → modify → delete)
- **Examples:** `test_user_lifecycle`, `test_service_account_full_lifecycle`, `test_policy_lifecycle`

### Error Case Tests
- **Count:** 12 tests
- **Purpose:** Verify error handling and validation
- **Examples:** `test_add_user_invalid_credentials`, `test_user_info_nonexistent_user`, `test_remove_remote_target_empty_arn`

---

## APIs with Best Test Coverage

**Most Exercised APIs** (used in 5+ tests):

| API | Test Count | Files | Coverage Type |
|-----|------------|-------|---------------|
| **AddUser** | 7 tests | `test_user_management.rs`, `test_policy_management.rs`, `test_group_management.rs` | Direct + Lifecycle + Cross-category |
| **RemoveUser** | 6 tests | `test_user_management.rs`, `test_policy_management.rs`, `test_group_management.rs` | Direct + Lifecycle + Cross-category |
| **ListUsers** | 5 tests | `test_user_management.rs` | Direct + Lifecycle |
| **RemoveRemoteTarget** | 5 tests | `test_remote_targets.rs` | Direct + Lifecycle + Error cases |

---

## APIs with Limited Test Coverage

**Least Exercised APIs** (used in 1-2 tests):

| API | Test Count | Issue |
|-----|------------|-------|
| **InfoCannedPolicy** | 1 test | Only tested in lifecycle |
| **UpdateServiceAccount** | 1 test | Only tested in lifecycle |
| **SetConfig** | 1 test | Limited modification testing |
| **GetBucketQuota** | 1 test | No lifecycle test |
| **SetBucketQuota** | 1 test | No lifecycle test |
| **ServerHealthInfo** | 0 tests | **NO COVERAGE** |
| **BucketScanInfo** | 0 tests | **NO COVERAGE** |
| **ClusterAPIStats** | 0 tests | **NO COVERAGE** |
| **Heal** | 0 tests | **NO COVERAGE** |
| **BackgroundHealStatus** | 0 tests | **NO COVERAGE** |

---

## Test Dependencies Visualization

```
User Management APIs
├── Used directly in test_user_management.rs (10 tests)
├── Used in test_policy_management.rs (1 test)
│   └── test_attach_detach_policy (with Policy APIs)
└── Used in test_group_management.rs (1 test)
    └── test_group_lifecycle (with Group APIs)

Policy Management APIs
├── Used directly in test_policy_management.rs (3 tests)
└── Cross-referenced in test_attach_detach_policy

Config Management APIs
└── Used directly in test_config_management.rs (4 tests)

Remote Target APIs
└── Used directly in test_remote_targets.rs (9 tests)
    └── 3 tests ignored (require multi-instance setup)

Server Info APIs
├── test_server_info.rs (3 tests)
│   ├── ServerInfo (2 tests ✅)
│   └── StorageInfo (1 test 🔴 ignored)
└── test_data_usage_info.rs (2 tests ✅)

Monitoring APIs (NEW)
└── NO TESTS YET ❌
    ├── ServerHealthInfo
    ├── BucketScanInfo
    └── ClusterAPIStats

Healing APIs (NEW)
└── NO TESTS YET ❌
    ├── Heal
    └── BackgroundHealStatus
```

---

## Recommendations for Improved Coverage

### 1. Add Dedicated Tests for Lifecycle-Only APIs
These APIs are only tested as part of larger lifecycle tests:

- **InfoCannedPolicy** - Add dedicated test
- **UpdateServiceAccount** - Add dedicated test before lifecycle
- **UpdateGroupMembers** - Add dedicated test
- **SetGroupStatus** - Add dedicated test

### 2. Add Integration Tests for New APIs
Priority order:

1. **ServerHealthInfo** - Add `test_server_health_info.rs`
2. **BucketScanInfo** - Add `test_bucket_scan_info.rs`
3. **ClusterAPIStats** - Add `test_cluster_api_stats.rs`
4. **Heal + BackgroundHealStatus** - Add `test_heal.rs`

### 3. Enhance Quota Management Tests
Currently only direct tests exist. Add:
- Lifecycle test: set quota → verify → modify → verify → delete

### 4. Add More Error Cases
APIs with limited error testing:
- **SetBucketQuota** - Test invalid quota values
- **UpdateServiceAccount** - Test invalid updates
- **SetConfig** - Test invalid config formats

### 5. Create Cross-Category Integration Tests
Examples:
- Service account with policy attachment
- Group with policy and quota management
- User with service account and policy

---

## Test File Impact Score

**Which test files provide the most coverage?**

| Test File | API Count | Total Tests | Impact Score |
|-----------|-----------|-------------|--------------|
| `test_user_management.rs` | 5 APIs | 10 tests | ⭐⭐⭐⭐⭐ High |
| `test_remote_targets.rs` | 4 APIs | 9 tests | ⭐⭐⭐⭐⭐ High |
| `test_service_accounts.rs` | 5 APIs | 5 tests | ⭐⭐⭐⭐ Medium-High |
| `test_config_management.rs` | 5 APIs | 4 tests | ⭐⭐⭐⭐ Medium-High |
| `test_policy_management.rs` | 6 APIs | 3 tests | ⭐⭐⭐ Medium |
| `test_server_info.rs` | 2 APIs | 3 tests | ⭐⭐ Low-Medium |
| `test_data_usage_info.rs` | 1 API | 2 tests | ⭐⭐ Low-Medium |
| `test_group_management.rs` | 4 APIs | 2 tests | ⭐⭐ Low-Medium |
| `test_quota_management.rs` | 2 APIs | 2 tests | ⭐ Low |
| `test_service_restart.rs` | 1 API | 2 tests | ⭐ Low |

---

## Update Checklist

When adding new tests, update:
- [ ] Quick Reference table (add new API row)
- [ ] Detailed Test File Analysis (add section if new file)
- [ ] Coverage Statistics (update percentages)
- [ ] Multi-API Test Analysis (if test uses 3+ APIs)
- [ ] APIs with Limited Coverage (remove if coverage improves)
- [ ] Test Dependencies Visualization (update tree)
- [ ] Test File Impact Score (update if new file)

---

**Document Maintenance:**
- Run `cargo test` to verify test counts
- Use `cargo test -- --list` to see all test names
- Update after adding new test files or tests
- Cross-reference with `TEST_COVERAGE.md` for consistency
