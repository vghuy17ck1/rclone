#!/usr/bin/env bash
# Print the Go version the KamPlexFS release is built with.
#
# This is the one upstream's build.yml builds its releases with, so it
# follows upstream when it is merged, or failing that the latest patch
# release of the Go version in go.mod. Run from the repository root.
set -euo pipefail

go=$(sed -n "s/^ *go: '\(.*\)'\$/\1/p" .github/workflows/build.yml | head -1)
if [[ -z $go ]]; then
    go="~$(sed -n 's/^go \([0-9]*\.[0-9]*\).*/\1/p' go.mod)"
fi
echo "$go"
