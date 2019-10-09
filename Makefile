.PHONY: lint
lint:
	@rustup component add clippy 2> /dev/null
	cargo clippy --all-features

.PHONY: test
test:
	cargo test
