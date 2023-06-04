test-docker-compose:
	cd ./tests && docker-compose down && docker-compose up -d
	export SERVER_ENDPOINT=localhost:9000 \
		&& export ACCESS_KEY="masoud" \
		&& export SECRET_KEY="Strong#Pass#2022" \
		&& export ENABLE_HTTPS=1 \
		&& export IGNORE_CERT_CHECK=1 \
		&& export SSL_CERT_FILE=./tests/public.crt \
		&& cargo nextest run --release
