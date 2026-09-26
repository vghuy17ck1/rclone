#!/usr/bin/env bash
# Check the release zips in build/ before they are published.
#
# usage: check.sh <version> <tags> <cgo> <listed> [run]
#
# Every rclone-<version>-*.zip must hold README.txt, README.html and
# rclone.1 with the KamPlexFS docs, and an rclone built as <version>
# with the build tags <tags> ("" for none), CGO_ENABLED=<cgo> and
# KamPlexFS support.
#
# The zips whose <os>-<arch> matches the regexp [run] are run as well,
# so the runner must be able to run them. They must report the
# version, list the tags <listed> ("none" for none) and KamPlexFS
# support and have a working mount command. rclone only lists cmount
# where it builds the cmount command, so not on linux without cgo.
# The others can only be checked from their build info and contents.
# Run from the repository root.
set -euo pipefail

version=$1
tags=$2
cgo=$3
listed=$4
run=${5:-^$}

shopt -s nullglob
zips=(build/rclone-"$version"-*.zip)
if [[ ${#zips[@]} == 0 ]]; then
    echo "No zips for $version in build/"
    exit 1
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
failed=0
fail() {
    echo "FAIL $name: $*"
    failed=1
}

for zip in "${zips[@]}"; do
    name=$(basename "$zip" .zip)
    platform=${name#"rclone-$version-"}
    echo "::group::$name"
    unzip -q "$zip" -d "$tmp"
    dir=$tmp/$name
    exe=$dir/rclone
    [[ -f $exe.exe ]] && exe=$exe.exe

    for file in "$exe" "$dir/README.txt" "$dir/README.html" "$dir/rclone.1"; do
        [[ -s $file ]] || fail "$(basename "$file") is missing"
    done
    grep -q "type = kamplexfs" "$dir/README.txt" || fail "README.txt has no KamPlexFS docs"

    # -trimpath leaves the ldflags out of the build info so the
    # version is looked for in the binary itself
    if ! info=$(go version -m "$exe" 2>/dev/null); then
        # go version can't read some formats (aix) but the build
        # info is in the binary as text
        info=$(LC_ALL=C grep -aoE $'build\t[^[:space:]]+' "$exe" | tr -d '\000' | sort -u | sed $'s/^/\t/')
    fi
    echo "$info" | grep -E $'\tbuild\t' || true
    LC_ALL=C grep -aqF "$version" "$exe" || fail "not built as $version"
    LC_ALL=C grep -aqF "github.com/rclone/rclone/backend/kamplexfs" "$exe" || fail "no kamplexfs backend"
    built_tags=$(sed -n $'s/^\tbuild\t-tags=//p' <<<"$info")
    [[ $built_tags == "$tags" ]] || fail "built with tags \"$built_tags\" not \"$tags\""
    built_cgo=$(sed -n $'s/^\tbuild\tCGO_ENABLED=//p' <<<"$info")
    [[ $built_cgo == "$cgo" ]] || fail "built with CGO_ENABLED=$built_cgo not $cgo"

    if [[ $platform =~ $run ]]; then
        out=$("$exe" version) || fail "rclone version failed"
        echo "$out"
        [[ $(head -1 <<<"$out") == "rclone $version" ]] || fail "rclone version doesn't say $version"
        grep -qxF -- "- go/tags: $listed" <<<"$out" || fail "rclone version doesn't list tags $listed"
        # the output is kept so grep -q can't make it fail with SIGPIPE
        out=$("$exe" help backends) || fail "rclone help backends failed"
        grep -qE '^ +kamplexfs ' <<<"$out" || fail "type = kamplexfs is missing"
        out=$("$exe" help backend s3) || fail "rclone help backend s3 failed"
        grep -qF '"KamPlexFS"' <<<"$out" || fail "provider = KamPlexFS is missing"
        "$exe" mount --help >/dev/null || fail "rclone mount --help failed"
        echo "Ran $name"
    else
        echo "Can't run $name here, checked its build only"
    fi
    rm -rf "$dir"
    echo "::endgroup::"
done

if [[ $failed != 0 ]]; then
    exit 1
fi
echo "Checked ${#zips[@]} zips"
