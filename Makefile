test-docker-compose:
	cd ./tests && docker-compose down && docker-compose up -d
	source ./.test.env && cargo test --release --all-features

test-docker-compose-nextest:
	cd ./tests && docker-compose down && docker-compose up -d
	source ./.test.env && cargo nextest run --release --all-features


# Useful for wireshark and similar
test-dc-no-certs:
	cd ./tests && docker-compose down && docker-compose \
		-f docker-compose.yml \
		-f docker-compose.http.yml \
		up -d

# Useful for wireshark and similar
# DOES NOT START DOCKER CONTAINER (keeping same network interface)
test-with-network-diagnostic:
	source ./.test.env && export ENABLE_HTTPS=false && cargo test --release --all-features


test-manual:
	./tests/start-server.sh \
		export SERVER_ENDPOINT=localhost:9000 \
		export ACCESS_KEY=minioadmin \
		export SECRET_KEY=minioadmin \
		export ENABLE_HTTPS=1 \
		export SSL_CERT_FILE=./tests/public.crt \
   	cargo test --verbose -- --nocapture --all-features

