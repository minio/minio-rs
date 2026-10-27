# Design rules — decide these before the first commit

Status: normative for new code in this repository. Each rule names its pattern,
points at an example in the tree, and says when to skip it. Cite them by number in
review. The SDK-wide conventions (the builder pattern for every S3 API, the typed
parameter pattern, the performance rules, the testing requirements and the
copyright header) are in the root [`CLAUDE.md`](../CLAUDE.md) and are not repeated
here. The places where the current code does not yet follow these rules are
tracked in [`DESIGN-RULES-TODO.md`](DESIGN-RULES-TODO.md).

| Rule                                        | Apply when                                                        |
| ------------------------------------------- | ----------------------------------------------------------------- |
| 1. Newtype a distinct value                 | two values share a primitive but are not interchangeable          |
| 2. Validate once, at the edge               | the value comes from outside the SDK                              |
| 3. Pick the invariant that deletes an error | a downstream `Result` covers a case that cannot happen            |
| 4. Encode a required order in the types     | a step must not run before another, and the wrong order is costly |
| 5. Route every operation through the state  | you applied rule 4                                                |
| 6. A policy parameter is an enum            | a parameter or return value selects a policy                      |
| 7. `Option`, not an in-band sentinel        | one value in the range means "absent"                             |
| 8. Start every new item private             | you add any item                                                  |
| 9. Pin the wire form with a test            | the type reaches an HTTP header, query string or XML body         |
| 10. One acquisition helper per lock         | shared state sits behind a lock                                   |
| 11. Errors are typed variants               | you add an error                                                  |
| 12. Comments follow ISO 24495-1             | every comment and doc string                                      |

## 1. Newtype a distinct value

**Pattern: newtype.** It cures what Fowler and Beck call primitive obsession.
`BucketName`, `ObjectKey`, `VersionId`, `UploadId`, `ETag` and `Region` are all
`String`, and nothing else about them is the same. As type aliases they would let
the compiler accept every swap, including the call that passes an object key where
a bucket name belongs.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub struct BucketName(String);
```

The types live in `src/s3/types/typed_parameters.rs`. Give each one `new`,
`as_str` and `into_inner`, plus the `TryFrom`, `FromStr` and `Display`
implementations listed in `CLAUDE.md`, so a caller can pass `&str`, `String` or the
type itself. Put an operation that fits one type only on that type, the way
`MaxKeys::MIN` and `MaxKeys::MAX` belong to `MaxKeys`.

Do not add `Deref`, `From<String>` or arithmetic operators: each hands the
distinction back, and `From<String>` skips validation, which is rule 2. **Skip** the
rule for a count or a size with one meaning in scope, such as `offset: u64` in
`GetObject`.

## 2. Validate once, at the edge

**Pattern: parse, don't validate.** A function that takes a validated type cannot
be called with an unvalidated value. `BucketName::new` runs `check_bucket_name`
once, and every builder, signer and URL helper downstream takes a `BucketName` and
never re-checks it. The edges are the public client methods, the response parsers
and the configuration values read from the environment.

The client method is where the conversion happens, so validation is charged to the
caller's `?` and not to `send()`:

```rust
pub fn get_object<B, O>(&self, bucket: B, object: O) -> Result<GetObjectBldr, ValidationErr>
```

The type guarantees exactly what its constructor checks, no more, so one type gets
one rule set. `BucketName::new` applies the strict S3 naming rules that AIStor
enforces, and it is the only constructor, which is why no builder checks a bucket
name again. An `ETag` is non-empty and nothing else, because the format is opaque and
differs per implementation.

Two constructors with two rule sets put the caller back where the rule started:
holding a validated value and still not knowing which operations accept it.

## 3. Pick the invariant that deletes an error

`MaxKeys` is bounded to 1..=1000 at construction, so the query string builder in
`src/s3/builders/list_objects.rs` writes `max_keys_value.to_string()` with no error
branch and no clamp. Ask which downstream error branch a stricter invariant would
delete, and prefer the invariant that deletes one.

The invariant must be enforced where the value is built, not asserted where it is
used. `MaxKeys::new` returns `ValidationErr::InvalidMaxKeys`; the alternative, a
`debug_assert` in front of an unchecked constructor, corrupts release builds
silently. A response parser builds its values the same way. `parse_buckets` in
`src/s3/response/list_buckets.rs` passes every listed name through
`BucketName::new` and returns the error, so a caller can pass a listed name
straight back to any operation.

## 4. Encode a required order in the types

**Pattern: typestate.** Strom and Yemini named it in 1986. Rust carried it before
1.0 and then dropped it, so it lives on as an idiom. Every S3 API builder gets it
from `typed_builder`: the builder's type parameters record which fields have been
set, so `build()` compiles only after the required ones are present.

```rust
pub type GetObjectBldr = GetObjectBuilder<(
    (MinioClient,),
    (),
    (),
    (),
    (BucketName,),
    (ObjectKey,),
    // ... one slot per optional field
)>;
```

Forgetting the bucket is a compile error, not a runtime error, and the alias names
the state that a caller receives from `MinioClient::get_object`. The second
transition is `S3Api::send(self)`, which consumes the request, so a built request
cannot be sent twice by accident.

Write a hand-rolled state type only when a required order is expensive to get
wrong. **Skip** an order that is obvious and cheap to get wrong; two states is
usually the useful maximum.

## 5. Route every operation through the state

A state type guards the operations that require it and nothing else. An operation
that reaches the same resource without holding one stays unguarded, however strict
the state machine looks. Here the resource is the HTTP client, and every operation
must reach it through `ToS3Request::to_s3request` followed by `S3Request::execute`.

That route is what applies the region lookup, SigV4 signing, the connection pool
and the retry policy. A builder that called `reqwest` directly would skip all four,
so the fields of `S3Request` are `pub(crate)`. Prefer a mechanism to a list: a list
of operations in a doc comment rots, and nothing fails when it is incomplete.

One public method does not follow the rule yet. `MinioClient::get_object_fast`
uses the HTTP client directly, so it skips the hooks and the region lookup.
[`DESIGN-RULES-TODO.md`](DESIGN-RULES-TODO.md) tracks the decision on it. Do not
add a second such method.

## 6. A policy parameter is an enum

`CopyObject` carries `metadata_directive: Option<Directive>`, so
`.metadata_directive(Directive::Replace)` reads at the call site, where
`.replace_metadata(true)` would not. `Directive::Copy` and `Directive::Replace`
name the two policies the S3 header allows, and a named enum leaves room for a
third value if the API grows one. The same holds for a return value and for the
algorithm selector `ChecksumAlgorithm`.

This rule is about a bool that selects a policy. A bool that answers a question
stays a bool: `Size::is_known`, `BucketName::is_empty`.

## 7. `Option`, not an in-band sentinel

A sentinel sits in the same range as real data, so a missing check reads as a real
value. `Size` in `src/s3/object_content.rs` is the model: `Size::Unknown` is a
variant, not `u64::MAX`, and `Size::value` hands back an `Option<u64>` at the one
place a number is needed.

When a wire format forces a sentinel, do three things: name it, give the type a
predicate, and document which values are reserved. The empty `Region` is the
crate's example. `BaseUrl` stores an empty region to mean "unspecified", the
`Deserialize` implementation maps an empty string to `Region::new_empty` rather
than rejecting it, and `Region::is_empty` is the predicate that every reader uses.
A new type does not get a second such case without a reason on the wire.

## 8. Start every new item private

`dead_code` never fires on a `pub` item, so unused public functions accumulate
unseen, and in a library every one of them is a compatibility promise. Declare a
new item private, `pub(crate)` or `pub(super)`, and widen it only for a caller that
needs it. `ObjectKey::new_unchecked` and the fields of `S3Request` are
`pub(crate)` for that reason.

The rule governs the item you add, not the crate's intended API. The SDK exports a
public surface on purpose, re-exported through `src/s3/mod.rs`; an item joins it by
decision, never by default.

## 9. Pin the wire form with a test

The contract with the server is the bytes on the wire: the request path, the query
string, the headers, the XML body and the SigV4 signature. A newtype over `String`
is free on the wire, but nothing enforces that, so write the test in the same
commit. Build the request and assert on it, the way
`src/s3/builders/get_object.rs` does:

```rust
let req = test_client()
    .get_object("test-bucket", "test-object")
    .unwrap()
    .enable_checksum(true)
    .build()
    .to_s3request()
    .unwrap();
assert_eq!(req.headers.get(X_AMZ_CHECKSUM_MODE).map(String::as_str), Some("ENABLED"));
```

A pin is only as good as its values: cover the absent case as well as the present
one, the percent-encoded characters (`/`, space, `+`, non-ASCII), and the empty
string where the format allows it. Signing is pinned the same way, by asserting
that a fixed request and a fixed timestamp produce the same signature, as the tests
in `src/s3/signer.rs` do. Changing a query parameter name, a header name or an XML
element name breaks compatibility with deployed servers and is not a refactor.

## 10. One acquisition helper per lock

Shared state behind a lock gets one function that takes the lock, and nothing else
touches the field. `get_signing_key` in `src/s3/signer.rs` is the only reader and
writer of `signing_key_cache`: it takes the read lock on the fast path, computes
outside the lock on a miss, and takes the write lock briefly to store the result.

The credential providers do not follow the rule yet. Each one touches its
`RwLock<Option<CachedCredentials>>` in several places, and
[`DESIGN-RULES-TODO.md`](DESIGN-RULES-TODO.md) tracks the work to give each
provider one acquisition helper. New shared state follows the
`get_signing_key` shape from the start.

Two rules keep that helper honest. Never hold a lock across an `.await`, because
the task can be parked while the guard is alive and other tasks are blocked for the
whole network round trip. When two locks exist, fix one order between them and
never take the reverse.

## 11. Errors are typed variants

`ValidationErr` and `Error` in `src/s3/error.rs` are `thiserror` enums, so a caller
can match `ValidationErr::InvalidBucketName` or `Error::S3Server` and act on it. A
formatted string does neither. The split is itself a contract: `ValidationErr` is
raised before any request is sent, and `Error` covers what happened on or after the
wire.

A variant names a condition a caller can act on, not a call site. Reuse the variant
that fits before adding one. Mark a public error enum `#[non_exhaustive]` when you
introduce it, so adding a variant later does not break callers that match on it.

## 12. Comments follow ISO 24495-1

Doc comments and code comments are writing for a human, so they meet the same plain
language standard as the documents in `docs/`.

- Start with the identifier, in the present tense: `// get_signing_key returns ...`.
  State the contract: inputs, return value, error cases. One to three sentences.
- Use short sentences and common words, one idea each. The reader may not be a
  native speaker.
- Describe the code as it is now: no bug history, no "used to", no reference to
  what the code replaced. Keep the rationale and the invariant, which are what stop
  the next reader from simplifying the code back.
- Make every pronoun point at one thing. When "it" or "this" has two candidate
  nouns in the preceding clauses, write the noun instead.
- Document what a method costs when the cost is not visible: the peak memory of a
  method that buffers a whole object, and the streaming alternative to use instead.
- When there is nothing non-obvious to say, write nothing. That is not licence to
  drop the doc comment on a public item, whose contract is always worth stating.

## Reading list

- Strom and Yemini, _Typestate_, IEEE TSE, 1986. Rule 4.
- Fowler and Beck, _Refactoring_, 1999. Rule 1.
- Yaron Minsky, _Make illegal states unrepresentable_, around 2011. Rules 1, 4, 6, 7.
- Alexis King, _Parse, don't validate_, 2019. Rules 2 and 3.
- ISO 24495-1, _Plain language_. Rule 12.
