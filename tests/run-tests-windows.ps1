# Set environment variables to run tests on play.min.io
$Env:SERVER_ENDPOINT = "http://localhost:9000/"
$Env:ACCESS_KEY = if ($Env:MINIO_ROOT_USER) { $Env:MINIO_ROOT_USER } else { "minioadmin" }
$Env:SECRET_KEY = if ($Env:MINIO_ROOT_PASSWORD) { $Env:MINIO_ROOT_PASSWORD } else { "minioadmin" }
$Env:ENABLE_HTTPS = "false"
$Env:MINIO_SSL_CERT_FILE = "./tests/public.crt"
$Env:IGNORE_CERT_CHECK = "false"
$Env:SERVER_REGION = ""

# Run tests
cargo test -- --nocapture

# run one specific test and show stdout
# cargo test --test test_bucket_exists -- --nocapture

# run tests with ring instead of default-crypto
# cargo test --no-default-features --features "default-tls,ring" -- --nocapture

