# rclone for KamPlexFS

This is a fork of [rclone](https://github.com/rclone/rclone) which adds
support for KamPlexFS servers:

- `provider = KamPlexFS` in the s3 backend - see
  [docs/content/s3.md](docs/content/s3.md#kamplexfs)
- `type = kamplexfs`, a backend for the KamPlexFS JSON REST API - see
  [docs/content/kamplexfs.md](docs/content/kamplexfs.md)

Everything else is upstream rclone.

## Branches

| Branch | Based on | Purpose |
|---|---|---|
| `kamplexfs/vX.Y-stable` | upstream tag `vX.Y.Z` | Release branch. Every push is released. |
| `kamplexfs/master` | upstream `master` | Development. Shows early whether the fork still applies to the next upstream release. |
| `master` | upstream `master` | Plain upstream, left alone. |

Work on the fork's features lands on `kamplexfs/master` and the stable
branch, usually as a pull request into each.

## Releases

`.github/workflows/kamplexfs-release.yml` runs on every push to a
`kamplexfs/vX.Y-stable` branch, a direct commit or a merged pull
request. It runs the fork's tests, builds portable zips and publishes
a GitHub release. It never runs for `kamplexfs/master` or any other
branch.

The zips are built by `.github/workflows/kamplexfs-build.yml` the way
upstream builds its releases: upstream's `bin/cross-compile.go` run by
`make cross`, with the build tags, cgo settings and FUSE libraries of
each job in upstream's `build.yml`. So `rclone mount` is built in
everywhere upstream has it, with `-tags cmount` on Windows and macOS.
The CI workflow runs the same build on every push to
`kamplexfs/master`, so a release build is known to work before it is
needed.

- Releases are tagged `<upstream version>-kamplexfs.<N>`, e.g.
  `v1.75.1-kamplexfs.1`, `v1.75.1-kamplexfs.2`, ... and
  `v1.75.2-kamplexfs.1` once upstream v1.75.2 is merged. The upstream
  version comes from the `VERSION` file. `rclone version` shows the
  tag.
- A commit which is already released isn't released again, so the
  workflow can be re-run from the Actions tab safely.
- The zips are laid out like the official ones, with `rclone` (or
  `rclone.exe`), `README.txt`, `README.html` and `rclone.1`, for every
  platform upstream releases (windows, macOS (`osx`), linux, freebsd,
  netbsd, openbsd, plan9, solaris and aix), plus `SHA256SUMS`. The
  README is the manual made from this branch's docs by
  `make -o rcdocs doc`. There are no .deb/.rpm packages.
- Before anything is published `.github/kamplexfs/check.sh` checks
  every zip's contents, version, build tags, cgo setting and KamPlexFS
  support, and runs those the runner can (linux ones with qemu) to
  check `rclone version`, `rclone help backends` and
  `rclone mount --help`. On Windows it mounts a directory as `X:`,
  reads it and unmounts it.
- `rclone selfupdate` installs official rclone, which would replace
  the build and lose the KamPlexFS support, so it refuses to run
  unless given `--force-upstream-update`. `rclone selfupdate --check`
  still works but the versions it reports are upstream releases.
- Notes for a release go in
  `.github/kamplexfs/release-notes/<tag>.md`, e.g.
  `v1.75.1-kamplexfs.2.md`, and are put at the top of the release
  notes followed by the list of commits since the last release.

To build and check zips locally (this needs pandoc, and nfpm from
`make release_dep_linux` for linux):

```sh
make -o rcdocs doc
make -o doc cross TAG=v1.75.1-kamplexfs.0 BUILD_FLAGS='-include "^linux/amd64"' GOTAGS=cmount
.github/kamplexfs/check.sh v1.75.1-kamplexfs.0 cmount 0 none '^linux-amd64$'
git checkout -- MANUAL.* rclone.1 docs cmd lib
```

## Bringing in upstream changes

Use the **kamplexfs sync upstream** workflow from the Actions tab.

**A new upstream patch release**, e.g. v1.75.2: mode `merge`,
upstream ref `v1.75.2`, branch `kamplexfs/v1.75-stable`. It opens a
pull request; merging it makes the release `v1.75.2-kamplexfs.1`.

**Upstream development**: mode `merge`, upstream ref `master`, branch
`kamplexfs/master`, open_pr off if you just want it pushed.

**A new upstream minor release**, e.g. v1.76.0: mode `new-stable`,
upstream ref `v1.76.0`, branch `kamplexfs/v1.76-stable`, from branch
`kamplexfs/v1.75-stable` (or `kamplexfs/master`). This starts the new
branch from the upstream tag and cherry-picks the fork's own commits
onto it, then pushes it, which releases it.

The workflow runs the fork's tests before pushing anything. If
upstream conflicts with the fork it stops and lists the conflicting
files; do the same merge or cherry-pick on your machine, resolve the
conflicts and push. `.github/kamplexfs/sync-upstream.sh` is the
script the workflow runs and works locally too:

```sh
.github/kamplexfs/sync-upstream.sh merge v1.75.2 kamplexfs/v1.75-stable
git push origin sync-work:kamplexfs/v1.75-stable
```

### The KAMPLEXFS_SYNC_TOKEN secret

Upstream merges often change files in `.github/workflows`, and the
default `GITHUB_TOKEN` isn't allowed to push those. Create a
fine-grained personal access token for this repository with
**Contents**, **Pull requests** and **Workflows** read and write, and
save it as the repository secret `KAMPLEXFS_SYNC_TOKEN`. Pull requests
and pushes made with it also start the CI and release workflows,
which ones made with `GITHUB_TOKEN` don't.

## Keeping upstream merges clean

The fork's code is kept out of upstream files as far as possible:

- `backend/s3/kamplexfs.go`, `backend/s3/kamplexfs_test.go` and
  `backend/s3/provider/KamPlexFS.yaml` - the s3 provider
- `backend/kamplexfs/` - the JSON backend
- `cmd/selfupdate/kamplexfs.go` and its test - the `selfupdate` guard
- `docs/content/kamplexfs.md`, `docs/data/backends/kamplexfs.yaml`
- `.github/kamplexfs/`, `.github/workflows/kamplexfs-*.yml` and this
  file

The only edits to upstream files are these small hooks, which are the
only places a merge can conflict:

- `backend/s3/s3.go`: the `kpx` field in `Fs`, the call to
  `f.kamplexfsSetup` in `NewFs`, and one-line calls in `Precision`,
  `ModTime`, `SetModTime` and `prepareUpload`
- `backend/s3/providers.go`: appending `kamplexfsOptions` in
  `addProvidersToInfo`
- `backend/all/all.go`: the import of `backend/kamplexfs`
- `cmd/selfupdate/selfupdate.go`: the calls to `kamplexfsCheckNote` in
  the command and `kamplexfsGuard` in `InstallUpdate`
- `docs/content/s3.md` and `docs/content/docs.md`: the KamPlexFS
  entries
- `bin/make_manual.py`: `kamplexfs.md` in the list of docs

Upstream's own workflows only run in `rclone/rclone`, so they are
skipped in this fork.

## Testing

The fork's unit tests need no server:

```sh
go test ./backend/s3/ ./backend/kamplexfs/ ./cmd/selfupdate/
```

Set `CI=1` to skip upstream's `selfupdate` tests which download
rclone.

They include rclone's full backend conformance suite run against an
in-memory fake of the JSON API in direct-fs, packed-volume and
pre-upgrade server modes. The first two make and remove their own
buckets.

To run the conformance suite against a real server, configure the
remotes `TestKamPlexFS` (`type = kamplexfs`) and `TestS3KamPlexFS`
(`type = s3`, `provider = KamPlexFS`) and run

```sh
go test -v ./backend/kamplexfs -remote TestKamPlexFS:bucket
go test -v ./backend/s3 -remote TestS3KamPlexFS:
```
