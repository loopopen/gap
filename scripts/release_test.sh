#!/bin/sh

set -eu

script=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/release.sh
fail=0

ok() {
	echo "ok $1"
}

bad() {
	echo "FAIL $1" >&2
	fail=1
}

expect_status() {
	want=$1
	name=$2
	shift 2
	set +e
	err=$("$@" 2>&1)
	status=$?
	set -e
	if [ "$status" -ne "$want" ]; then
		bad "$name: exit $status, want $want"
		printf '%s\n' "$err" >&2
	else
		ok "$name"
	fi
}

check_version() {
	want=$1
	ver=$2
	set +e
	err=$(
		RELEASE_SH_TEST=1
		# shellcheck disable=SC1090
		. "$script"
		version=$ver
		validate_version 2>&1
	)
	status=$?
	set -e
	if [ "$want" = ok ] && [ "$status" -ne 0 ]; then
		bad "version $ver: exit $status, want 0"
		printf '%s\n' "$err" >&2
	elif [ "$want" = bad ] && [ "$status" -ne 2 ]; then
		bad "version $ver: exit $status, want 2"
		printf '%s\n' "$err" >&2
	else
		ok "version $ver"
	fi
}

init_repo() {
	repo=$(mktemp -d)
	trap 'rm -rf "$repo"' EXIT
	cd "$repo"
	git init -q
	git config user.email release-test@example.com
	git config user.name release-test
	printf 'init\n' >README
	git add README
	git commit -qm init
	git branch -M main
	git init -q --bare "$repo/origin.git"
	git remote add origin "$repo/origin.git"
	git push -q origin main
	git fetch -q origin
	RELEASE_SH_TEST=1
	# shellcheck disable=SC1090
	. "$script"
}

test_main_matches_origin() {
	init_repo
	require_main
}

test_rejects_other_branch() {
	init_repo
	git checkout -q -b feature
	require_main
}

test_rejects_detached_head() {
	init_repo
	git checkout -q --detach
	require_main
}

test_rejects_main_ahead_of_origin() {
	init_repo
	git commit -q --allow-empty -m ahead
	require_main
}

test_rejects_missing_origin_main() {
	init_repo
	git update-ref -d refs/remotes/origin/main
	require_main
}

test_rollback_removes_release_commit_and_new_tags() {
	init_repo
	git tag keep-me
	orig_head=$(git rev-parse HEAD)
	tags_before=$(mktemp)
	git for-each-ref --format='%(refname)' refs/tags | sort >"$tags_before"
	printf 'release\n' >>README
	git add README
	git commit -qm 'chore: release v1.0.1'
	git tag v1.0.1
	git tag storage/xmysql/v1.0.1
	rollback_release
	if [ "$(git rev-parse HEAD)" != "$orig_head" ]; then
		echo "HEAD was not restored" >&2
		exit 1
	fi
	if ! git rev-parse --verify --quiet refs/tags/keep-me >/dev/null; then
		echo "pre-existing tag was deleted" >&2
		exit 1
	fi
	if git rev-parse --verify --quiet refs/tags/v1.0.1 >/dev/null; then
		echo "release tag was kept" >&2
		exit 1
	fi
	if git rev-parse --verify --quiet refs/tags/storage/xmysql/v1.0.1 >/dev/null; then
		echo "module release tag was kept" >&2
		exit 1
	fi
	if grep -q release README; then
		echo "release edit survived rollback" >&2
		exit 1
	fi
	rm -f "$tags_before"
}

for version in \
	v1.0.0 \
	v1.0.1 \
	v1.2.3 \
	v1.10.0 \
	v1.0.10 \
	v1.0.0-0 \
	v1.0.0-1 \
	v1.0.0-beta \
	v1.0.0-beta.0 \
	v1.0.0-beta.1 \
	v1.0.0-beta.10 \
	v1.0.0-0.1 \
	v1.0.0-rc.1 \
	v1.0.0-alpha.beta \
	v1.0.0-alpha.beta.1 \
	v1.0.0-beta-1 \
	v1.0.0-01beta \
	v1.0.0-beta.1+exp.sha \
	v1.0.0+build \
	v1.0.0+001 \
	v1.0.0-1.2.3
do
	check_version ok "$version"
done

for version in \
	v1.0.0-01 \
	v1.0.0-00 \
	v1.0.0-beta.01 \
	v1.0.0-01.1 \
	v1.0.0-rc.1.02 \
	v1.0.0-01+build \
	v1.00.0 \
	v1.0.00 \
	v1.0.0- \
	v1.0.0-beta. \
	v1.0.0-beta..1 \
	v1.0 \
	v2.0.0 \
	1.0.0 \
	V1.0.0
do
	check_version bad "$version"
done

expect_status 0 main-matches-origin test_main_matches_origin
expect_status 1 other-branch test_rejects_other_branch
expect_status 1 detached-head test_rejects_detached_head
expect_status 1 main-ahead test_rejects_main_ahead_of_origin
expect_status 1 missing-origin-main test_rejects_missing_origin_main
expect_status 0 rollback test_rollback_removes_release_commit_and_new_tags

if [ "$fail" -ne 0 ]; then
	exit 1
fi
