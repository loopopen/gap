.PHONY: test tidy tag-gap tag-xgorm tag-xmysql tag-xkafka tag-xconfluent tag-xrabbitmq tag-all

MODULES := \
	. \
	broker/xkafka \
	broker/xconfluent \
	broker/xrabbitmq \
	storage/xgorm \
	storage/xmysql \
	examples/fiber-rabbitmq-mysql-example \
	examples/gin-kafka-mysql-example \
	examples/gin-kafka-mysql-example2 \
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
		(make tidy-modules) || exit 1; \
		(make test-modules) || exit 1; \
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

tag-xconfluent:
	$(call tag_func,./broker/xconfluent/options.go,broker/xconfluent/)

tag-xrabbitmq:
	$(call tag_func,./broker/xrabbitmq/options.go,broker/xrabbitmq/)

tag-all:
	@for target in tag-gap tag-xgorm tag-xmysql tag-xkafka tag-xconfluent tag-xrabbitmq; do \
		$(MAKE) --no-print-directory $$target tag="$(tag)" || exit 1; \
	done

tidy: tidy-modules
	@go mod tidy

push:
	git push && git push --tags
