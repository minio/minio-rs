test-docker-compose-nextest:
	cd ./tests && docker-compose down && docker-compose up -d
	source ./.test.env && cargo nextest run --release

test-docker-compose:
	cd ./tests && docker-compose down && docker-compose up -d
	source ./.test.env && cargo test --release
