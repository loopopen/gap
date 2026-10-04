.PHONY: test tidy

MODULES := \
	. \
	broker/xkafka \
	broker/xrabbitmq \
	storage/xgorm \
	storage/xmysql \
	examples/fiber-rabbitmq-mysql-example \
	examples/gin-kafka-mysql-example \
	examples/kafka-gorm-postgres-example \
	examples/rabbitmq-gorm-mysql-example \
	examples/rabbitmq-mysql-example

test-modules:
	@for dir in $(MODULES); do \
		(cd $$dir && GOWORK=off go test ./...) || exit 1; \
	done

tidy-modules:
	@for dir in $(MODULES); do \
		(cd $$dir && GOWORK=off go mod tidy) || exit 1; \
	done

gen-modules:
	@for dir in $(MODULES); do \
		(cd $$dir && go generate ./...) || exit 1; \
	done

build-ui:
	cd ./internal/dashboard/app && npm run build

define tag_func
	@if [ -z "$(tag)" ]; then \
		grep -oE 'version = "v[0-9]+\.[^"]*' $(1) | cut -d'"' -f2; \
	else \
		make test-modules; \
		make tidy-modules; \
		sed -i '' "s/= \"v[0-9]\{1,\}\.[^\"]*\"/= \"$(tag)\"/" $(1); \
		git commit -am"chore: $(2)$(tag)"; \
		git tag $(2)$(tag); \
	fi
endef

tag-gap:
	$(call tag_func,./gap.go,)

tag-xgorm:
	$(call tag_func,./storage/xgorm/options.go,storage/xgorm/)

tag-xmysql:
	$(call tag_func,./storage/xmysql/options.go,storage/xmysql/)

tag-xkafka:
	$(call tag_func,./broker/xkafka/options.go,broker/xkafka/)

tag-xrabbitmq:
	$(call tag_func,./broker/xrabbitmq/options.go,broker/xrabbitmq/)

tidy: tidy-modules
	@go mod tidy

push:
	git push && git push --tags