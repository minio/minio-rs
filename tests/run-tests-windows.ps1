# Set environment variables to run tests on play.min.io
$Env:SERVER_ENDPOINT = "https://play.min.io/"
$Env:ACCESS_KEY = "minioadmin"
$Env:SECRET_KEY = "minioadmin"
$Env:ENABLE_HTTPS = "true"
$Env:SSL_CERT_FILE = "./tests/public.crt"
$Env:IGNORE_CERT_CHECK = "false"
$Env:SERVER_REGION = ""

# Run tests
cargo test