ROOT_MODULE := github.com/loopopen/gap

PLUGIN_MODULES := \
	broker/xconfluent \
	broker/xkafka \
	broker/xrabbitmq \
	storage/xgorm \
	storage/xmysql \
	storage/xpostgres

EXAMPLE_MODULES := \
	examples/rabbitmq-kafka-mysql-example-fiber \
	examples/gin-kafka-mysql-example \
	examples/gin-kafka2-postgres-example \
	examples/kafka-gorm-postgres-example \
	examples/rabbitmq-gorm-mysql-example \
	examples/rabbitmq-mysql-example-fiber \
	examples/rabbitmq-mysql-example-worker

TEST_MODULES := storage/contracttest

MODULES := . $(PLUGIN_MODULES) $(EXAMPLE_MODULES) $(TEST_MODULES)

# test uses go.work. test-isolated builds each module alone, as a consumer would.
# test-race covers the runtime, pump, storage, and the RabbitMQ channel pool.
# test-storage runs the MySQL, PostgreSQL, and RabbitMQ contracts.

GAP_TEST_MYSQL_PORT ?= 3307
GAP_TEST_POSTGRES_PORT ?= 5433
GAP_TEST_RABBITMQ_PORT ?= 5673
STORAGE_COMPOSE := docker compose -f storage/contracttest/docker-compose.yml

.PHONY: \
	test test-isolated test-race test-storage test-storage-running \
	tidy generate build-ui versions release tidy-modules gen-modules

test:
	@for dir in $(MODULES); do \
		(cd $$dir && go test ./...) || exit 1; \
	done

test-isolated:
	@GOWORK=off go test ./...
	@for dir in $(PLUGIN_MODULES); do \
		(cd $$dir && \
			trap 'go mod edit -dropreplace=$(ROOT_MODULE) >/dev/null 2>&1 || true' EXIT; \
			go mod edit -replace=$(ROOT_MODULE)=../..; \
			GOWORK=off go test ./...) || exit 1; \
	done
	@for dir in $(EXAMPLE_MODULES) $(TEST_MODULES); do \
		(cd $$dir && GOWORK=off go test ./...) || exit 1; \
	done

test-race:
	go test -race -count=1 . ./internal/pump/ ./storage/xsql/
	@for dir in broker/xrabbitmq storage/xmysql storage/xpostgres storage/xgorm; do \
		(cd $$dir && go test -race -count=1 ./...) || exit 1; \
	done

test-storage:
	@set -eu; \
	trap '$(STORAGE_COMPOSE) down -v' EXIT INT TERM; \
	$(STORAGE_COMPOSE) up -d --wait; \
	GAP_STORAGE_CONTRACT=1 \
	GAP_TEST_MYSQL_DSN="root:root@tcp(127.0.0.1:$(GAP_TEST_MYSQL_PORT))/gap_contract?parseTime=true" \
	GAP_TEST_POSTGRES_DSN="postgres://postgres:postgres@127.0.0.1:$(GAP_TEST_POSTGRES_PORT)/gap_contract?sslmode=disable" \
	GAP_TEST_RABBITMQ_URL="amqp://gap:gap@127.0.0.1:$(GAP_TEST_RABBITMQ_PORT)/" \
	$(MAKE) --no-print-directory test-storage-running

test-storage-running:
	@(cd storage/contracttest && go test -run Contract -count=1 ./...)
	@for dir in storage/xmysql storage/xpostgres; do \
		(cd $$dir && go test -run TestIntegrationPoolOwnership -count=1 .) || exit 1; \
	done

tidy: tidy-modules

generate: gen-modules

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
