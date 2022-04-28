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
	cargo build

.PHONY: prepare-pinot-cluster
prepare-pinot-cluster:  ## Set-up basic pinot cluster
	docker-compose up -d pinot-zookeeper pinot-controller pinot-broker pinot-server

.PHONY: prepare-pinot-db
prepare-pinot-db:  ## Set-up pinot table and test data
	docker exec -it pinot-client-rust-pinot-controller bin/pinot-admin.sh AddTable   \
		-tableConfigFile /db/score_sheet/offline_table.json   \
		-schemaFile /db/score_sheet/schema.json -exec
	docker exec -it pinot-client-rust-pinot-controller bin/pinot-admin.sh LaunchDataIngestionJob \
		-jobSpecFile /db/score_sheet/job_spec.yaml

.PHONY: prepare-pinot
prepare-pinot:  ## Set-up basic pinot cluster and create table and test data
	$(MAKE) prepare-pinot-cluster
	sleep 30
	$(MAKE) prepare-pinot-db

destroy-pinot:  ## Tear down basic pinot cluster
	docker-compose down

.PHONY: test
test: prepare-pinot  ## Run tests
	-cargo test
	$(MAKE) destroy-pinot

