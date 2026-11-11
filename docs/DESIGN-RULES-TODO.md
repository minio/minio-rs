# Design rules: open work

Findings from an audit of the tree against [`DESIGN-RULES.md`](DESIGN-RULES.md).
Each item is scoped to be one pull request. The rule number in brackets says which
rule the item restores.

Batches 1 to 3 are ordered by value. Batches 4 to 8 can be taken in any order.
Items marked **breaking** change the public API, so they should land in one
release together.

Size is a rough guide: S is under an hour, M is half a day, L is longer.

## Batch 1: behavior

These change what the SDK does, not just how it reads.

- [x] **Settle what `BucketName` promises, then delete the late checks.** [rule 2]
      Done: `BucketName::new` applies the strict S3 rules, the 28 late checks in 17
      builder files are gone, and `get_object_fast` no longer validates twice.
- [ ] **Replace `status: bool` with a `ReplicationStatus` enum.** [rule 6]
      `get_text_result(element, "Status")? == "Enabled"` turns `Disabled`, an
      unknown value and a typo all into `false`. Parse the three cases and return
      an error for an unknown one. Size: M. **breaking**. Files:
      `types/all_types.rs:1221,1231,1348,1353,1370,1383`, the same lines under
      `types/replication/`, and the `new(status: bool)` constructors at
      `types/all_types.rs:1159,1175`. Do this after the duplicate types are
      removed (batch 3), or the edit has to be made twice.
- [ ] **Give credential providers the client's configured HTTP client.** [rule 5]
      `assume_role.rs:208`, `iam.rs:112` and `web_identity.rs:156` each build their
      own `reqwest::Client`, so `ssl_cert_file`, `ignore_cert_check` and the
      connection pool apply to S3 calls but not to the STS exchange. A `Provider`
      is built before the `MinioClient` exists, so this needs a way to hand the
      client's `reqwest::Client` to the provider at build time. Size: L.
- [ ] **Decide the future of `get_object_fast`.** [rule 5]
      `client/mod.rs:1329` reaches `self.http_client` without `ToS3Request`, so
      hooks never run and the region is forced to `us-east-1` whatever the client
      is configured to do. Either route it through `S3Request` with the overhead it
      actually needs, or keep it and rename it so the bypass is visible at the call
      site. Size: M.

## Batch 2: use the types that already exist

- [ ] **Type the conditional request fields as `ETag`.** [rule 1] 18 fields are
      `Option<String>`: `get_object.rs:55,57`, `put_object.rs:240,248,678,686`,
      `stat_object.rs:59,61`, `rename_object.rs:58,62,72,76`,
      `copy_object.rs:888,890,1052,1054`. Size: M. **breaking**.
- [ ] **Type the version fields as `VersionId`.** [rule 1] `get_object.rs:45`,
      `get_object_attributes.rs:73`, `response/delete_object.rs:47,54,56`. Other
      builders already use `Option<VersionId>` for the same field. Size: S.
      **breaking**.
- [ ] **Type `UploadPart::upload_id` as `UploadId`.** [rule 1]
      `put_object.rs:407`. Removes the runtime emptiness check at
      `put_object.rs:482`. Size: S. **breaking**.
- [ ] **Add a `PartNumber` newtype.** [rules 1, 3] The range `1..=MAX_MULTIPART_COUNT`
      is checked inline at `copy_object.rs:105` and `put_object.rs:488`. A newtype
      deletes both branches. Size: S. **breaking**.
- [ ] **Split `UploadPart` from `PutObject`.** [rule 4] One struct serves both
      operations, so the fields the multipart path requires are optional and
      checked at runtime (`put_object.rs:405-409`, `put_object.rs:481-494`). Two
      structs move both checks to compile time. Size: M. **breaking**. Do this
      together with the `UploadId` and `PartNumber` items above.
- [ ] **Return `Region` from `region_response`.** [rule 1]
      `response/get_region.rs:45` returns `String`. Size: S. **breaking**.

## Batch 3: remove what should not be there

- [x] **Retire the relaxed bucket name path.** [rules 6, 8]
      Done: `check_bucket_name` takes no `strict` parameter, the relaxed regex is
      gone, and `BucketName::new` is the only constructor.
- [ ] **Delete one of the two copies of the replication types.** [rule 8]
      `types/replication/` (667 lines, 9 types) and `types/all_types.rs` hold
      identical definitions of `ReplicationConfig`, `ReplicationRule`,
      `Destination`, `Metrics`, `ReplicationTime`, `SourceSelectionCriteria`,
      `EncryptionConfig`, `AccessControlTranslation` and `ObjectLockConfig`. Which
      one a caller gets depends on the order of `types/mod.rs:78` and
      `types/mod.rs:84`, because the explicit re-export shadows the glob. A fix to
      one copy does not reach the other. Size: M.
- [ ] **Make `MaxKeysValue` private, or remove the deferral.** [rules 2, 8]
      `builders/list_objects.rs:32` is `pub` with a doc comment saying callers
      should not construct it, and `MaxKeysValue::Pending` carries an unvalidated
      `u16` until `to_s3request`. Size: S. **breaking**.
- [ ] **Remove the three `#[allow(dead_code)]` items or their allowance.** [rule 8]
      `signer.rs:345`, `signer.rs:751`, `types/minio_error_response.rs:110`. Size: S.
- [ ] **Delete the vacuous accessors on validated types.** [rule 3] Six `is_empty`
      methods document themselves as "should never happen after validation", plus
      the matching `len` methods, 12 in total:
      `types/typed_parameters.rs:122,514,649,772,907,1025`. Keep `Region::is_empty`,
      which is the predicate for a documented sentinel. Size: S. **breaking**.

## Batch 4: errors

- [ ] **Mark the public error enums `#[non_exhaustive]`.** [rule 11] `ValidationErr`,
      `Error`, `S3ServerError`, `IoError` and `NetworkError` in `s3/error.rs`.
      Adding a variant is a breaking change until this lands. Size: S. **breaking**.
- [ ] **Replace the `StrError` catch-all where a condition deserves a variant.**
      [rule 11] `builders/inventory.rs:144` (empty YAML definition) and
      `builders/update_object_encryption.rs:98` (missing KMS key ARN). Callers
      cannot match either today. Size: S.
- [ ] **Decide where transport errors live.** [rule 11] `ValidationErr` carries
      `IOError`, `XmlParseError` and `HttpError` (`s3/error.rs:28-35`), so it is not
      the pre-request error type that `DESIGN-RULES.md` describes. Either move the
      three variants into `Error` or soften the claim in rule 11. Size: M.
      **breaking** if the variants move.

## Batch 5: sentinels in the response accessors

- [ ] **Make `object_size` honest.** [rule 7] `response_traits.rs:236` returns `0`
      for a missing or unparsable header. `object_size_checked` sits beside it and
      returns an error. Keep one. Size: S. **breaking**.
- [ ] **Separate absent from malformed in `version_id`.** [rule 7]
      `response_traits.rs:192` maps a present but invalid header to `None`. Size: S.
- [ ] **Stop defaulting annotation fields.** [rule 7] A missing name becomes `""`
      and a missing or unparsable size becomes `0`
      (`response/list_object_annotations.rs:71-74`). Size: S.
- [ ] **Return `Option<Credentials>` from `Provider::fetch`.** [rule 7]
      `creds.rs:78` returns `Credentials::empty()` to mean "not primed yet". The
      convention is documented, so this is the mildest item here. Size: M.
      **breaking**.

## Batch 6: locks

- [ ] **One acquisition helper per credential cache, and one poison policy.**
      [rule 10] Five providers repeat the same three-site pattern with no helper:
      `assume_role.rs:229,238,246`, `iam.rs:135,318,326`,
      `web_identity.rs:175,184,192`, `file.rs:164,168`, `creds.rs:278,292,373`.
      `signer.rs:260,279` carries on when the lock is poisoned; every provider
      panics on `.unwrap()`. Pick one policy and apply it. Size: M.
- [ ] **Make a refresh single-flight.** [rule 10] `ensure_credentials` reads, drops
      the guard, then refreshes, so two tasks can run the same STS exchange at the
      same time. Size: M. Do this inside the helper from the previous item.

## Batch 7: tests

43 of 58 builder files have no unit test. Split the work by group; each group is
one pull request.

- [ ] **Bucket configuration PUTs.** [rule 9] `put_bucket_lifecycle`,
      `put_bucket_replication`, `put_bucket_notification`, `put_bucket_encryption`,
      `put_bucket_policy`, `put_bucket_tagging`, `put_bucket_versioning`. These
      write XML bodies that no offline test reads. Size: M.
- [ ] **Object configuration PUTs.** [rule 9] `put_object_retention`,
      `put_object_lock_config`, `put_object_legal_hold`, `put_object_tagging`.
      Size: S.
- [ ] **Presigned URL and form data.** [rule 9] `get_presigned_object_url`,
      `get_presigned_policy_form_data`. Pin the query parameters and the policy
      document. Size: M.
- [ ] **The remaining GET and DELETE builders.** [rule 9] The rest of the 43.
      Assert on `to_s3request` output the way
      `builders/get_object.rs:175-199` does. Size: M.

## Batch 8: comments

- [ ] **Fix the two comments that state the opposite of the code.** [rule 12]
      `put_object.rs:407` and `put_object.rs:409` carry `// force required` on
      `#[builder(default, setter(into))]`, which makes the field optional. The other
      75 uses of that comment are correct. Size: S.
- [ ] **Remove the commented-out `Error` enum.** [rule 12]
      `bucket_policy_config.rs:9-22`. Size: S.
- [ ] **Triage the 9 TODO comments in `src/`.** [rule 12] Each becomes an issue, a
      fix, or a deletion. `types/s3_request.rs:46` ("is this really needed?
      Investigate") and `utils.rs:1247` ("consider fixing or removing this test")
      are notes to self. Size: S.
