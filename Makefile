ROOT_MODULE := github.com/loopopen/gap/v2

PLUGIN_MODULES := \
	broker/xconfluent \
	broker/xkafka \
	broker/xrabbitmq \
	storage/xgorm \
	storage/xmysql \
	storage/xpostgres

EXAMPLE_MODULES := \
	examples/fiber-rabbitmq-kafka-mysql-example \
	examples/fiber-rabbitmq-mysql-example \
	examples/gin-kafka-mysql-example \
	examples/gin-kafka2-postgres-example \
	examples/kafka-gorm-postgres-example \
	examples/rabbitmq-gorm-mysql-example \
	examples/rabbitmq-mysql-example

MODULES := . $(PLUGIN_MODULES) $(EXAMPLE_MODULES)

.PHONY: \
	test test-isolated tidy generate build-ui versions release \
	test-modules test-modules-isolated tidy-modules gen-modules

test: test-modules

test-isolated: test-modules-isolated

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

gen-modules:
	@for dir in $(MODULES); do \
		(cd $$dir && go generate ./...) || exit 1; \
	done

build-ui:
	cd internal/dashboard/app && npm run build

versions:
	@./scripts/release.sh versions $(PLUGIN_MODULES)

release:
	@./scripts/release.sh release "$(VERSION)" $(PLUGIN_MODULES)

# make release VERSION=v2.0.1
# make release-module MODULE=storage/xgorm VERSION=v2.0.1