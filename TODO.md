# TODO Items for MinIO Rust SDK

This document contains all TODO, FIXME, XXX, and HACK items found in the codebase.

## Critical TODOs

### Copyright Headers Missing
- **src/madmin/client.rs:1** - No copyright notice. Please check all files
- **src/madmin/response/update_management/cancel_server_update.rs:1** - No copyright

### Lazy Parsing / Refactoring Needed
- **src/madmin/response/user_management/set_user_req.rs:24** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/user_management/revoke_tokens_ldap.rs:24** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/user_management/add_user.rs:34** - Why is this function not replaced by the macro impl_from_madmin_response?
- **src/madmin/response/update_management/cancel_server_update.rs:10** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/idp_config/add_or_update_idp_config.rs:45** - Why is this function not replaced by the macro impl_from_madmin_response?
- **src/madmin/response/idp_config/check_idp_config.rs:34** - Why is this function not replaced by the macro impl_from_madmin_response?
- **src/madmin/response/pool_management/decommission_pool.rs:24** - Why is from_madmin_response different from the others?
- **src/madmin/response/lock_management/force_unlock.rs:24** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/pool_management/cancel_decommission_pool.rs:24** - Why is from_madmin_response different from the others?
- **src/madmin/response/idp_config/delete_idp_config.rs:45** - Why is this function not replaced by the macro impl_from_madmin_response?
- **src/madmin/response/site_replication/site_replication_resync.rs:29** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/monitoring/profile.rs:33** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/monitoring/download_profiling_data.rs:34** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/policy_management/add_azure_canned_policy.rs:25** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/site_replication/site_replication_peer_join.rs:30** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/site_replication/site_replication_peer_iam_item.rs:29** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/policy_management/remove_azure_canned_policy.rs:25** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/profiling/profile.rs:33** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/server_info/data_usage_info.rs:80** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/profiling/download_profiling_data.rs:34** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/server_info/get_api_logs.rs:46** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/server_info/inspect.rs:29** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/remote_targets/list_remote_targets.rs:49** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/server_info/storage_info.rs:33** - Did you forget to refactor from_madmin_response here?
- **src/madmin/response/remote_targets/remove_remote_target.rs:27** - Did you forget to refactor from_madmin_response here?

## Admin API (madmin) TODOs

### Response Handling
- **src/madmin/response/user_management/info_access_key.rs:52** - Fetching the credentials is a recurring pattern, consider a trait such as HasBucket with name HasCredentials
- **src/madmin/response/user_management/add_service_account.rs:64** - What is the difference between "credentials" and "response_data.credentials"
- **src/madmin/response/bucket_metadata/export_bucket_metadata.rs:37** - Is this method really needed? Body is already public through HasMadminFields
- **src/madmin/response/rebalancing/rebalance_start.rs:26** - Can we just have an id method on RebalanceStartResponse that lazily parses the body when needed?
- **src/madmin/response/configuration/set_log_config.rs:37** - Does this function need to exist? Is it because the go code also has one?
- **src/madmin/response/batch/mod.rs:28** - Why are the functions all in this file instead of separate files like other responses?
- **src/madmin/response/iam_management/mod.rs:24** - Why are all functions in this module and not in separate files like other modules?
- **src/madmin/response/configuration/set_config_kv.rs:39** - Would a case insensitive compare be better here?
- **src/madmin/response/idp_config/check_idp_config.rs:47** - Other code uses "serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)", what is better?
- **src/madmin/response/configuration/reset_log_config.rs:37** - Is this function really needed?
- **src/madmin/response/monitoring/top_locks.rs:40** - Should we wrap the original error here? Check this for all "source: None"
- **src/madmin/response/monitoring/get_license_info.rs:35** - Is LicenseInfo only used here? Should it be moved to here?
- **src/madmin/response/policy_management/attach_policy.rs:33** - Why can't we make the response data lazy?
- **src/madmin/response/site_replication/site_replication_peer_bucket_ops.rs:55** - Make status an enum? What does the go sdk do?
- **src/madmin/response/server_info/cluster_api_stats.rs:30** - Are these camelCase? rename_all still needed?
- **src/madmin/response/site_replication/site_replication_peer_bucket_meta.rs:44** - Consider making status return also the detail for efficiency
- **src/madmin/response/service_control/service_trace.rs:49** - Please double check with S3 how streaming responses are handled, how do API calls in s3 handle the request?

## S3 API TODOs

### Validation and Utilities
- **src/s3/utils.rs:65** - Creating a new Crc object is expensive, we should cache it
- **src/s3/utils.rs:695** - Validates given bucket name. S3Express has slightly different rules for bucket names
- **src/s3/utils.rs:763** - Validates given object name. S3Express has slightly different rules for object names
- **src/s3/utils.rs:901** - Use this while adding API to set tags

### Client Implementation
- **src/s3/client.rs:542** - Why-oh-why first collect into a vector and then iterate to a stream?
- **src/s3/client/delete_bucket.rs:81** - Consider how to handle this (dummy request)
- **src/s3/client/delete_bucket.rs:161** - Consider how to handle this (dummy request)
- **src/s3/client/copy_object.rs:47** - todo!()
- **src/s3/client/copy_object.rs:49** - .upload_part_copy("bucket-name", "object-name", "TODO")

### Builders
- **src/s3/builders/copy_object.rs:373** - todo!() - Nothing to do
- **src/s3/builders/copy_object.rs:383** - todo!() - Nothing to do
- **src/s3/builders/copy_object.rs:529** - Redundant use of bucket and object
- **src/s3/builders/delete_bucket_notification.rs:61** - Consider const body
- **src/s3/builders/delete_object_lock_config.rs:57** - Consider const body
- **src/s3/builders/delete_objects.rs:288** - TODO
- **src/s3/builders/put_bucket_policy.rs:44** - Consider PolicyConfig struct
- **src/s3/builders/put_bucket_versioning.rs:124** - This seems inconsistent: `None`: No change to the current versioning status
- **src/s3/builders/put_object_legal_hold.rs:84** - Consider const payload with precalculated md5

### Response
- **src/s3/response/get_presigned_object_url.rs:13** - TODO
- **src/s3/multimap_ext.rs:98** - todo!() - This never happens

## Tests TODOs

### Bucket Tests
- **tests/test_bucket_encryption.rs:29** - This gives a runtime error
- **tests/test_bucket_replication.rs:169** - Compare replication configs
- **tests/test_bucket_policy.rs:47** - Create a proper comparison of the retrieved config and the provided config

## Examples TODOs

### Bucket Lifecycle
- **examples/bucket_lifecycle.rs:35** - TODO
- **examples/bucket_lifecycle.rs:64** - TODO
- **examples/bucket_lifecycle.rs:74** - TODO

## Benchmarks TODOs

- **benches/s3/api_benchmarks.rs:79** - Setup permissions to allow replication

## Common Library TODOs

- **common/src/example.rs:48** - Or should this be NONE??

---

## Summary

**Total TODOs: 72**

### By Category:
- **Lazy Parsing / Refactoring**: 25 items
- **S3 API**: 18 items
- **Admin API (madmin)**: 18 items
- **Tests**: 3 items
- **Examples**: 3 items
- **Copyright Headers**: 2 items
- **Common Library**: 1 item
- **Benchmarks**: 1 item

### Priority Recommendations:
1. **High Priority**: Fix missing copyright headers (2 items)
2. **High Priority**: Complete lazy parsing refactoring for consistency (25 items)
3. **Medium Priority**: S3Express bucket/object name validation (2 items)
4. **Medium Priority**: Address S3 client and builder TODOs (18 items)
5. **Low Priority**: Documentation and example improvements (6 items)
