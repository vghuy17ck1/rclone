#!/usr/bin/env bash
# Build a portable rclone release zip for one OS/architecture.
#
# usage: package.sh <version> <goos> <goarch> [goarm]
#
# Makes build/rclone-<version>-<os>-<arch>.zip laid out like the
# official rclone zips: rclone(.exe), README.txt, README.html and
# rclone.1 in a directory of the same name. Run from the repository
# root.
set -euo pipefail

version=$1
goos=$2
goarch=$3
goarm=${4:-}

# Names as used by the official releases
os_name=$goos
[[ $goos == darwin ]] && os_name=osx
arch_name=$goarch
[[ -n $goarm ]] && arch_name=$goarch-v$goarm

name=rclone-$version-$os_name-$arch_name
dir=build/$name
rm -rf "$dir" "$dir.zip"
mkdir -p "$dir"

exe=rclone
[[ $goos == windows ]] && exe=rclone.exe

export CGO_ENABLED=0 GOOS=$goos GOARCH=$goarch
[[ -n $goarm ]] && export GOARM=$goarm
[[ $goarch == 386 ]] && export GO386=softfloat

syso=
if [[ $goos == windows ]]; then
    # Embed the version information and icon
    syso=resource_windows_$goarch.syso
    GOOS= GOARCH= go run bin/resource_windows.go -arch "$goarch" -version "$version" -syso "$syso"
fi

go build -trimpath -ldflags "-s -X github.com/rclone/rclone/fs.Version=$version" -o "$dir/$exe" .
[[ -n $syso ]] && rm -f "$syso"

cp MANUAL.txt "$dir/README.txt"
cp MANUAL.html "$dir/README.html"
cp rclone.1 "$dir/"

(cd build && zip -qr9 "$name.zip" "$name")
rm -rf "$dir"
echo "Made build/$name.zip"
