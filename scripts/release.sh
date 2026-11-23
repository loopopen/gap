#!/bin/sh

set -eu

if [ "${RELEASE_SH_TEST:-}" != 1 ]; then
	root_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
	cd "$root_dir"
fi

rollback_needed=0
orig_head=
tags_before=

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

require_main() {
	branch=$(git symbolic-ref --quiet --short HEAD) || {
		echo "release requires branch main" >&2
		exit 1
	}
	if [ "$branch" != main ]; then
		echo "release requires branch main, current branch is $branch" >&2
		exit 1
	fi
	if ! git rev-parse --verify --quiet refs/remotes/origin/main >/dev/null; then
		echo "origin/main is missing; fetch origin before releasing" >&2
		exit 1
	fi
	local_main=$(git rev-parse refs/heads/main)
	origin_main=$(git rev-parse refs/remotes/origin/main)
	if [ "$local_main" != "$origin_main" ]; then
		echo "local main ($local_main) does not match origin/main ($origin_main)" >&2
		exit 1
	fi
}

record_baseline() {
	orig_head=$(git rev-parse HEAD)
	tags_before=$(mktemp)
	git for-each-ref --format='%(refname)' refs/tags | sort >"$tags_before"
	echo "release baseline HEAD: $orig_head"
	echo "release baseline tags: $(wc -l <"$tags_before" | tr -d ' ')"
}

rollback_release() {
	echo "release failed; restoring the recorded baseline" >&2
	if [ -z "$orig_head" ]; then
		echo "no baseline HEAD was recorded" >&2
		return 1
	fi
	echo "git reset --hard $orig_head" >&2
	if [ -n "$tags_before" ] && [ -f "$tags_before" ]; then
		current_tags=$(mktemp)
		git for-each-ref --format='%(refname)' refs/tags | sort >"$current_tags"
		while IFS= read -r ref; do
			if ! grep -Fxq "$ref" "$tags_before"; then
				short=${ref#refs/tags/}
				echo "git tag -d $short" >&2
				git tag -d "$short"
			fi
		done <"$current_tags"
		rm -f "$current_tags"
	fi
	git reset --hard "$orig_head"
}

release_exit() {
	status=$?
	trap - EXIT
	if [ "$status" -ne 0 ] && [ "$rollback_needed" -eq 1 ]; then
		rollback_release || true
	fi
	if [ -n "$tags_before" ]; then
		rm -f "$tags_before"
	fi
	exit "$status"
}

invalid_version() {
	echo "invalid version: $version" >&2
	echo "expected a v1 semantic version such as v1.0.1 or v1.0.1-beta.1" >&2
	exit 2
}

validate_version() {
	pattern='^v1\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$'
	printf '%s\n' "$version" | grep -Eq "$pattern" || invalid_version

	prerelease=$(printf '%s\n' "$version" | sed -nE 's/^v1\.[0-9]+\.[0-9]+-([^+]*)(\+.*)?$/\1/p')
	[ -n "$prerelease" ] || return 0

	rest=$prerelease
	while [ -n "$rest" ]; do
		ident=${rest%%.*}
		case $rest in
			*.*) rest=${rest#*.} ;;
			*) rest= ;;
		esac
		# A numeric identifier may be 0 or start with 1-9. 01 is rejected.
		case $ident in
			*[!0-9]*) ;;
			0 | [1-9]*) ;;
			*) invalid_version ;;
		esac
	done
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

commit_release() {
	set -- "$(version_file .)"
	for module_dir in $plugin_modules; do
		set -- "$@" "$(version_file "$module_dir")" "$module_dir/go.mod"
		if [ -f "$module_dir/go.sum" ]; then
			set -- "$@" "$module_dir/go.sum"
		fi
	done
	git add -- "$@"
	if git diff --cached --quiet -- "$@"; then
		echo "release files already set; no commit needed"
		return
	fi
	git commit -m "chore: release $version" -- "$@"
}

tag_release() {
	for module_dir in . $plugin_modules; do
		full_tag=$(tag_name "$module_dir" "$version")
		git tag "$full_tag"
		echo "created tag: $full_tag"
	done
}

release() {
	trap release_exit EXIT
	validate_version
	require_main
	require_clean_tree "working tree must be clean before releasing"

	for module_dir in . $plugin_modules; do
		validate_module "$module_dir"
		check_tag "$(tag_name "$module_dir" "$version")"
	done

	record_baseline

	make --no-print-directory tidy
	require_clean_tree "go mod tidy changed the working tree; commit those changes first"
	make --no-print-directory test-isolated

	rollback_needed=1
	update_version .
	for module_dir in $plugin_modules; do
		prepare_plugin "$module_dir"
	done
	make --no-print-directory test-isolated

	commit_release
	tag_release

	if [ "${RELEASE_DRY_RUN:-}" = 1 ]; then
		echo "dry run: skipped push"
		return 0
	fi

	set -- "HEAD:refs/heads/$branch" "refs/tags/$version"
	for module_dir in $plugin_modules; do
		set -- "$@" "refs/tags/$(tag_name "$module_dir" "$version")"
	done
	git push --atomic origin "$@"
}

if [ "${RELEASE_SH_TEST:-}" = 1 ]; then
	return 0
fi

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
