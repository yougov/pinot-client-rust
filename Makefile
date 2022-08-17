.DEFAULT_GOAL := test

# Self-documenting Makefile
# https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: update-rustup
update-rustup:
	rustup update

.PHONY: install-clippy
install-clippy:
	rustup component add clippy

.PHONY: setup
setup: update-rustup install-clippy

.PHONY: clean
clean:  ## Clean target directory
	cargo clean

.PHONY: lint
lint: touch  ## Run lint
	cargo clippy

.PHONY: build
build:  ## Build project
	cargo build --all-targets --all-features

.PHONY: create-pinot
create-pinot:  ## Set-up basic pinot cluster
	docker-compose up -d pinot-zookeeper pinot-controller pinot-broker pinot-server

.PHONY: populate-pinot
populate-pinot:  ## Set-up pinot table and test data
	cd scripts/test/pinot/;	./populate-db.sh

.PHONY: prepare-pinot
prepare-pinot:  ## Set-up basic pinot cluster and create table and test data
	$(MAKE) create-pinot
	$(MAKE) populate-pinot

destroy-pinot:  ## Tear down basic pinot cluster
	docker-compose down

.PHONY: test
test: prepare-pinot  ## Run tests
	-cargo test --all-targets --all-features
	$(MAKE) destroy-pinot

