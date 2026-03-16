.PHONY: test

build-ui:
	cd ./internal/dashboard/app && npm run build

define tag_func
	@if [ -z "$(tag)" ]; then \
		grep -oE 'version = "v[0-9]+\.[^"]*' $(1) | cut -d'"' -f2; \
	else \
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
