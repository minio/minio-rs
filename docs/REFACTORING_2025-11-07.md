# Directory Structure Refactoring - November 7, 2025

## Overview

Reorganized the MinIO Admin API codebase from a flat structure into a hierarchical, category-based structure to improve maintainability and scalability.

## Motivation

With 87/198 APIs implemented (44%) and projection to reach 198 files per directory at completion:
- **Navigation difficulty** - Finding specific APIs among 198+ files
- **Scalability concerns** - Flat structure doesn't scale well
- **Maintenance burden** - Hard to see related functionality

## New Structure

```
src/madmin/
├── types/                    (domain types - stays mostly flat)
├── builders/
│   ├── configuration/        (13 APIs)
│   ├── group_management/     (4 APIs)
│   ├── healing/              (2 APIs)
│   ├── idp_config/           (5 APIs)
│   ├── monitoring/           (5 APIs)
│   ├── policy_management/    (14 APIs)
│   ├── quota_management/     (2 APIs)
│   ├── remote_targets/       (4 APIs)
│   ├── server_info/          (8 APIs)
│   ├── service_control/      (4 APIs)
│   ├── user_management/      (21 APIs)
│   └── mod.rs               (re-exports all categories)
├── client/                   (same structure as builders)
└── response/                 (same structure as builders)
```

## Categories

| Category | APIs | Description |
|----------|------|-------------|
| **user_management** | 21 | User accounts, service accounts, access keys, tokens |
| **policy_management** | 14 | IAM policies, Azure policies, LDAP policies |
| **configuration** | 13 | Server config, config KV, config history, log config |
| **server_info** | 8 | Server status, storage, health, usage, logs, inspect |
| **monitoring** | 5 | Metrics, profiling, locks, KMS, license |
| **idp_config** | 5 | Identity provider configuration (OIDC, LDAP) |
| **service_control** | 4 | Start, stop, freeze, unfreeze services |
| **remote_targets** | 4 | Bucket replication target management |
| **group_management** | 4 | User group operations |
| **healing** | 2 | Data healing and background heal status |
| **quota_management** | 2 | Bucket quota limits |

## Benefits

1. **Logical Grouping** - Related APIs are co-located
2. **Scalability** - 198 files → 11 categories with ~13 files each (average)
3. **Better Navigation** - Clear hierarchy in IDEs
4. **Easier Maintenance** - Changes to a feature area are localized
5. **Module Organization** - Each category has its own mod.rs

## Migration Details

- **Files Moved**: 81 files in builders/, 81 in client/, 78 in response/
- **Method Used**: `git mv` to preserve history
- **Breaking Changes**: None - all public APIs remain accessible via re-exports
- **Tests**: All 216 unit tests pass ✅

## Future APIs

With this structure, the remaining 111 APIs will be added to existing categories:
- KMS & Encryption: 18 more APIs
- Site Replication: 15 APIs
- Batch Operations: 8 APIs
- Tiering: 8 APIs
- Performance Testing: 5 APIs
- And more...

## Developer Impact

### Before
```rust
use minio::madmin::builders::AddUser;
```

### After (same - no breaking changes)
```rust
use minio::madmin::builders::AddUser;  // Still works!
// OR be more explicit:
use minio::madmin::builders::user_management::AddUser;
```

All existing code continues to work due to re-exports in the main `mod.rs` files.

## Conclusion

This refactoring positions the codebase for sustainable growth from 87 to 198+ APIs while improving developer experience and code maintainability.
