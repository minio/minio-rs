test-docker-compose:
	cd ./tests && docker-compose down && docker-compose up -d
	source ./.test.env && cargo test --release

test-docker-compose-nextest:
	cd ./tests && docker-compose down && docker-compose up -d
	source ./.test.env && cargo nextest run --release

test-manual:
	./tests/start-server.sh \
		export SERVER_ENDPOINT=localhost:9000 \
		export ACCESS_KEY=minioadmin \
		export SECRET_KEY=minioadmin \
		export ENABLE_HTTPS=1 \
		export SSL_CERT_FILE=./tests/public.crt \
   	cargo test --verbose -- --nocapture
