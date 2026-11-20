ROOT_MODULE := github.com/loopopen/gap

PLUGIN_MODULES := \
	broker/xconfluent \
	broker/xkafka \
	broker/xrabbitmq \
	broker/xredisstreams \
	storage/xgorm \
	storage/xmysql \
	storage/xpostgres

EXAMPLE_MODULES := \
	examples/rabbitmq-kafka-mysql-example-fiber \
	examples/kafka-mysql-example-gin \
	examples/kafka2-postgres-example-gin \
	examples/kafka-gorm-postgres-example \
	examples/rabbitmq-gorm-mysql-example \
	examples/rabbitmq-mysql-example-fiber \
	examples/redis-mysql-example-fiber \
	examples/rabbitmq-mysql-example-worker

TEST_MODULES := storage/contracttest

MODULES := . $(PLUGIN_MODULES) $(EXAMPLE_MODULES) $(TEST_MODULES)

.PHONY: \
	test test-isolated test-storage tidy generate build-ui versions release \
	test-modules test-modules-isolated tidy-modules gen-modules

test: test-modules

test-isolated: test-modules-isolated

test-storage:
	@set -eu; \
	compose='docker compose -f storage/contracttest/docker-compose.yml'; \
	trap '$$compose down -v' EXIT INT TERM; \
	$$compose up -d --wait; \
	GAP_STORAGE_CONTRACT=1 \
	GAP_TEST_MYSQL_DSN="root:root@tcp(127.0.0.1:$${GAP_TEST_MYSQL_PORT:-3307})/gap_contract?parseTime=true" \
	GAP_TEST_POSTGRES_DSN="postgres://postgres:postgres@127.0.0.1:$${GAP_TEST_POSTGRES_PORT:-5433}/gap_contract?sslmode=disable" \
	$(MAKE) --no-print-directory test-storage-running

.PHONY: test-storage-running
test-storage-running:
	@(cd storage/contracttest && go test -run Contract -count=1 ./...)

tidy: tidy-modules

generate: gen-modules

test-modules:
	@for dir in $(MODULES); do \
		(cd $$dir && go test ./...) || exit 1; \
	done

test-modules-isolated:
	@(GOWORK=off go test ./...)
	@for dir in $(PLUGIN_MODULES); do \
		(cd $$dir && \
			trap 'go mod edit -dropreplace=$(ROOT_MODULE) >/dev/null 2>&1 || true' EXIT; \
			go mod edit -replace=$(ROOT_MODULE)=../..; \
			GOWORK=off go test ./...) || exit 1; \
	done
	@for dir in $(EXAMPLE_MODULES); do \
		(cd $$dir && GOWORK=off go test ./...) || exit 1; \
	done
	@for dir in $(TEST_MODULES); do \
		(cd $$dir && GOWORK=off go test ./...) || exit 1; \
	done

tidy-modules:
	@GOWORK=off go mod tidy
	@for dir in $(PLUGIN_MODULES); do \
		(cd $$dir && \
			trap 'go mod edit -dropreplace=$(ROOT_MODULE) >/dev/null 2>&1 || true' EXIT; \
			go mod edit -replace=$(ROOT_MODULE)=../..; \
			GOWORK=off go mod tidy) || exit 1; \
	done
	@for dir in $(EXAMPLE_MODULES); do \
		(cd $$dir && GOWORK=off go mod tidy) || exit 1; \
	done
	@for dir in $(TEST_MODULES); do \
		(cd $$dir && GOWORK=off go mod tidy) || exit 1; \
	done

gen-modules:
	@for dir in $(MODULES); do \
		(cd $$dir && go generate ./...) || exit 1; \
	done

build-ui:
	cd internal/dashboard/app && npm install && npm run build

versions:
	@./scripts/release.sh versions $(PLUGIN_MODULES)

release:
	@./scripts/release.sh release "$(VERSION)" $(PLUGIN_MODULES)
