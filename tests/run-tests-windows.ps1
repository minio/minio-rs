# Set environment variables to run tests on play.min.io
$Env:SERVER_ENDPOINT = "http://localhost:9000/"
$Env:ACCESS_KEY = "minioadmin"
$Env:SECRET_KEY = "minioadmin"
$Env:ENABLE_HTTPS = "false"
$Env:SSL_CERT_FILE = "./tests/public.crt"
$Env:IGNORE_CERT_CHECK = "false"
$Env:SERVER_REGION = ""

# Run tests

# run all s3-express tests
#cargo test -- :s3express:

# run all s3 tests
cargo test -- :s3:

# run one specific test and show stdout
# cargo test --test test_get_presigned_object_url -- --nocapture