.PHONY: test tidy tag push

ROOT_MODULE := github.com/loopopen/gap
RELEASE_GOPROXY := https://proxy.golang.org,direct
RELEASE_GONOSUMDB := $(ROOT_MODULE),$(ROOT_MODULE)/*

TAG_MODULES := \
	storage/xgorm \
	storage/xmysql \
	broker/xkafka \
	broker/xconfluent \
	broker/xrabbitmq

MODULES := \
	. \
	broker/xkafka \
	broker/xconfluent \
	broker/xrabbitmq \
	storage/xgorm \
	storage/xmysql \
	examples/fiber-rabbitmq-mysql-example \
	examples/gin-kafka-mysql-example \
	examples/gin-kafka2-postgres-example \
	examples/kafka-gorm-postgres-example \
	examples/rabbitmq-gorm-mysql-example \
	examples/rabbitmq-mysql-example

test-modules:
	@for dir in $(MODULES); do \
		(cd $$dir && go test ./...) || exit 1; \
	done

test-modules-isolated:
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

# The aggregate tag target enables this when the root module and all
# submodules are released at the same version. Standalone submodule targets
# preserve their existing github.com/loopopen/gap requirement.
define tag_func
	@set -e; \
	prefix="$(2)"; \
	module="$(ROOT_MODULE)"; \
	if [ -n "$$prefix" ]; then \
		module="$$module/$${prefix%/}"; \
	fi; \
	if [ -z "$(tag)" ]; then \
		version=$$(sed -nE 's/.*version = "(v[^"]+)".*/\1/p' "$(1)"); \
		printf '%s: %s\n' "$$module" "$$version"; \
	else \
		full_tag="$${prefix}$(tag)"; \
		if git rev-parse --quiet --verify "refs/tags/$$full_tag" >/dev/null; then \
			echo "tag already exists: $$full_tag" >&2; \
			exit 1; \
		fi; \
		set -- "$(1)"; \
		if [ -n "$$prefix" ] && [ "$(SYNC_ROOT_VERSION)" = "1" ]; then \
			module_dir="$${prefix%/}"; \
			(cd "$$module_dir" && go mod edit -require="$(ROOT_MODULE)@$(tag)"); \
			(cd "$$module_dir" && \
				GOWORK=off \
				GOPROXY="$(RELEASE_GOPROXY)" \
				GONOSUMDB="$(RELEASE_GONOSUMDB)" \
				go mod tidy); \
			set -- "$$@" "$$module_dir/go.mod" "$$module_dir/go.sum"; \
		fi; \
		sed -i.bak -E 's/(version = ")[^"]+(".*)/\1$(tag)\2/' "$(1)"; \
		rm -f "$(1).bak"; \
		actual_version=$$(sed -nE 's/.*version = "(v[^"]+)".*/\1/p' "$(1)"); \
		if [ "$$actual_version" != "$(tag)" ]; then \
			echo "failed to update version in $(1): got $$actual_version" >&2; \
			exit 1; \
		fi; \
		if git diff --quiet -- "$$@"; then \
			echo "release files already set; no commit needed: $$module"; \
		else \
			git add -- "$$@"; \
			git commit -m "chore: $$full_tag" -- "$$@"; \
		fi; \
		git tag "$$full_tag"; \
		echo "created tag: $$full_tag"; \
	fi
endef

tag-check:
	@if ! printf '%s\n' "$(tag)" | grep -Eq '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?(\+[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?$$'; then \
		echo "invalid tag version: $(tag)" >&2; \
		echo "expected a semantic version such as v0.3.0 or v0.3.0-beta.1" >&2; \
		exit 1; \
	fi
	@if [ -n "$$(git status --porcelain)" ]; then \
		echo "working tree must be clean before creating tags" >&2; \
		git status --short >&2; \
		exit 1; \
	fi
	$(MAKE) tidy-modules
	@if [ -n "$$(git status --porcelain)" ]; then \
		echo "go mod tidy changed the working tree; commit those changes first" >&2; \
		git status --short >&2; \
		exit 1; \
	fi
	$(MAKE) test-modules

ifneq ($(strip $(tag)),)
ifeq ($(SKIP_TAG_CHECKS),)
TAG_CHECK_DEPS := tag-check
endif
endif

tag-gap: $(TAG_CHECK_DEPS)
	$(call tag_func,./gap.go,)

tag-xgorm: $(TAG_CHECK_DEPS)
	$(call tag_func,./storage/xgorm/options.go,storage/xgorm/)

tag-xmysql: $(TAG_CHECK_DEPS)
	$(call tag_func,./storage/xmysql/options.go,storage/xmysql/)

tag-xkafka: $(TAG_CHECK_DEPS)
	$(call tag_func,./broker/xkafka/options.go,broker/xkafka/)

tag-xconfluent: $(TAG_CHECK_DEPS)
	$(call tag_func,./broker/xconfluent/options.go,broker/xconfluent/)

tag-xrabbitmq: $(TAG_CHECK_DEPS)
	$(call tag_func,./broker/xrabbitmq/options.go,broker/xrabbitmq/)

tag: $(TAG_CHECK_DEPS)
	@if [ -n "$(tag)" ]; then \
		for prefix in "" $(addsuffix /,$(TAG_MODULES)); do \
			full_tag="$${prefix}$(tag)"; \
			if git rev-parse --quiet --verify "refs/tags/$$full_tag" >/dev/null; then \
				echo "tag already exists: $$full_tag" >&2; \
				exit 1; \
			fi; \
		done; \
	fi
	@$(MAKE) --no-print-directory tag-gap tag="$(tag)" SKIP_TAG_CHECKS=1
	@if [ -n "$(tag)" ]; then \
		echo "publishing $(ROOT_MODULE) $(tag) before tagging submodules"; \
		git push origin HEAD; \
		git push origin "refs/tags/$(tag)"; \
	fi
	@for target in tag-xgorm tag-xmysql tag-xkafka tag-xconfluent tag-xrabbitmq; do \
		$(MAKE) --no-print-directory $$target tag="$(tag)" SKIP_TAG_CHECKS=1 SYNC_ROOT_VERSION=1 || exit 1; \
	done

tidy: tidy-modules
	@go mod tidy

push:
	git push && git push --tags
