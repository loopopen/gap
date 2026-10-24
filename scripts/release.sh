#!/bin/sh

set -eu

root_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root_dir"

usage() {
	echo "usage: $0 versions [plugin ...]" >&2
	echo "       $0 release VERSION [plugin ...]" >&2
	exit 2
}

version_file() {
	if [ "$1" = "." ]; then
		printf '%s\n' "gap.go"
	else
		printf '%s\n' "$1/options.go"
	fi
}

module_path() {
	(cd "$1" && GOWORK=off go list -m)
}

current_version() {
	sed -nE 's/^[[:space:]]*const version = "(v[^"]+)".*/\1/p' "$(version_file "$1")"
}

tag_name() {
	if [ "$1" = "." ]; then
		printf '%s\n' "$2"
	else
		printf '%s/%s\n' "$1" "$2"
	fi
}

validate_module() {
	module_dir=$1
	file=$(version_file "$module_dir")
	if [ ! -f "$module_dir/go.mod" ] || [ ! -f "$file" ]; then
		echo "invalid release module: $module_dir" >&2
		exit 1
	fi
	if [ -z "$(current_version "$module_dir")" ]; then
		echo "version constant not found in $file" >&2
		exit 1
	fi
}

print_versions() {
	for module_dir in . $plugin_modules; do
		validate_module "$module_dir"
		printf '%s: %s\n' "$(module_path "$module_dir")" "$(current_version "$module_dir")"
	done
}

require_clean_tree() {
	message=$1
	if [ -n "$(git status --porcelain)" ]; then
		echo "$message" >&2
		git status --short >&2
		exit 1
	fi
}

validate_version() {
	if ! printf '%s\n' "$version" | grep -Eq '^v1\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?(\+[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?$'; then
		echo "invalid version: $version" >&2
		echo "expected a v1 semantic version such as v1.0.1 or v1.0.1-beta.1" >&2
		exit 2
	fi
}

check_tag() {
	full_tag=$1
	if git show-ref --verify --quiet "refs/tags/$full_tag"; then
		echo "tag already exists locally: $full_tag" >&2
		exit 1
	fi
	remote_tag=$(git ls-remote --tags origin "refs/tags/$full_tag") || {
		echo "failed to check remote tag: $full_tag" >&2
		exit 1
	}
	if [ -n "$remote_tag" ]; then
		echo "tag already exists on origin: $full_tag" >&2
		exit 1
	fi
}

update_version() {
	file=$(version_file "$1")
	sed -i.bak -E 's/^([[:space:]]*const version = ")[^"]+(".*)$/\1'"$version"'\2/' "$file"
	rm -f "$file.bak"
	actual_version=$(current_version "$1")
	if [ "$actual_version" != "$version" ]; then
		echo "failed to update version in $file: got $actual_version" >&2
		exit 1
	fi
}

prepare_plugin() {
	module_dir=$1
	(
		cd "$module_dir"
		trap 'go mod edit -dropreplace="$root_module" >/dev/null 2>&1 || true' 0
		go mod edit -require="$root_module@$version"
		go mod edit -replace="$root_module=../.."
		GOWORK=off go mod tidy
		go mod edit -dropreplace="$root_module"
		trap - 0
	)
	update_version "$module_dir"
}

commit_module() {
	module_dir=$1
	full_tag=$(tag_name "$module_dir" "$version")
	file=$(version_file "$module_dir")
	set -- "$file"
	if [ "$module_dir" != "." ]; then
		set -- "$@" "$module_dir/go.mod"
		if [ -f "$module_dir/go.sum" ]; then
			set -- "$@" "$module_dir/go.sum"
		fi
	fi
	git add -- "$@"
	if git diff --cached --quiet -- "$@"; then
		echo "release files already set; no commit needed: $(module_path "$module_dir")"
	else
		git commit -m "chore: $full_tag" -- "$@"
	fi
	git tag "$full_tag"
	echo "created tag: $full_tag"
}

release() {
	validate_version
	branch=$(git symbolic-ref --quiet --short HEAD) || {
		echo "release requires a checked-out branch" >&2
		exit 1
	}
	require_clean_tree "working tree must be clean before releasing"

	for module_dir in . $plugin_modules; do
		validate_module "$module_dir"
		check_tag "$(tag_name "$module_dir" "$version")"
	done

	make --no-print-directory tidy
	require_clean_tree "go mod tidy changed the working tree; commit those changes first"
	make --no-print-directory test-isolated

	update_version .
	for module_dir in $plugin_modules; do
		prepare_plugin "$module_dir"
	done
	make --no-print-directory test-isolated

	commit_module .
	for module_dir in $plugin_modules; do
		commit_module "$module_dir"
	done

	set -- "HEAD:refs/heads/$branch" "refs/tags/$version"
	for module_dir in $plugin_modules; do
		set -- "$@" "refs/tags/$(tag_name "$module_dir" "$version")"
	done
	git push --atomic origin "$@"
}

command=${1:-}
case "$command" in
	versions)
		shift
		plugin_modules="$*"
		print_versions
		;;
	release)
		[ "$#" -ge 2 ] || usage
		version=$2
		shift 2
		plugin_modules="$*"
		root_module=$(module_path .)
		release
		;;
	*)
		usage
		;;
esac
