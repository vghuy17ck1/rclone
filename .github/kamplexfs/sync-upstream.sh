#!/usr/bin/env bash
# Bring upstream rclone changes into the KamPlexFS fork.
#
# usage:
#   sync-upstream.sh merge <upstream-ref> <branch>
#       Merge the upstream tag or branch into <branch>, e.g.
#         merge v1.75.2 kamplexfs/v1.75-stable
#         merge master kamplexfs/master
#
#   sync-upstream.sh new-stable <upstream-tag> <branch> <from-branch>
#       Start <branch> from the upstream tag and cherry-pick the fork's
#       own commits from <from-branch> onto it, e.g.
#         new-stable v1.76.0 kamplexfs/v1.76-stable kamplexfs/v1.75-stable
#
# The result is left checked out on the local branch "sync-work",
# ready to be pushed. The fork's branches are read from the "origin"
# remote. On a conflict it lists the conflicting files and exits with
# status 2, leaving the repository as it was before.
#
# Set UPSTREAM_URL to use a different upstream repository.
set -euo pipefail

UPSTREAM_URL=${UPSTREAM_URL:-https://github.com/rclone/rclone.git}
WORK=sync-work

die() {
    echo "ERROR: $*" >&2
    exit 1
}

conflict() {
    echo "::error::$1 conflicts in:"
    git diff --name-only --diff-filter=U | sed 's/^/  /'
    cat <<EOF

Resolve it on your machine instead:

  git remote add upstream $UPSTREAM_URL   # once
  git fetch upstream --tags
  $2

then fix the conflicts, commit and push.
EOF
}

# Fetch upstream branches and tags into their own namespaces so they
# can't clash with the fork's own branches and tags.
fetch_upstream() {
    if git remote get-url upstream >/dev/null 2>&1; then
        git remote set-url upstream "$UPSTREAM_URL"
    else
        git remote add upstream "$UPSTREAM_URL"
    fi
    git fetch --quiet --no-tags upstream \
        "+refs/heads/*:refs/remotes/upstream/*" \
        "+refs/tags/*:refs/upstream-tags/*"
}

# Resolve an upstream tag or branch name to a ref
upstream_ref() {
    if git rev-parse --verify --quiet "refs/upstream-tags/$1^{commit}" >/dev/null; then
        echo "refs/upstream-tags/$1"
    elif git rev-parse --verify --quiet "refs/remotes/upstream/$1" >/dev/null; then
        echo "refs/remotes/upstream/$1"
    else
        die "no upstream tag or branch called $1"
    fi
}

# Check out a fresh work branch at $1
start_work() {
    git checkout --quiet --detach
    git branch --quiet --force "$WORK" "$1"
    git checkout --quiet "$WORK"
}

merge() {
    local ref=$1 branch=$2
    git fetch --quiet origin "+refs/heads/$branch:refs/remotes/origin/$branch" || die "no branch $branch on origin"
    local upstream
    upstream=$(upstream_ref "$ref")
    start_work "origin/$branch"
    if git merge-base --is-ancestor "$upstream" HEAD; then
        echo "$branch already contains upstream $ref"
        exit 0
    fi
    if ! git merge --no-ff --no-edit -m "Merge upstream rclone $ref into $branch" "$upstream"; then
        git rev-parse --quiet --verify MERGE_HEAD >/dev/null || die "merge failed"
        conflict "Merging upstream $ref into $branch" "git checkout $branch && git merge $ref"
        git merge --abort
        exit 2
    fi
    echo "Merged upstream $ref into $branch:"
    git log --oneline --no-merges "origin/$branch..HEAD" | head -50
}

new_stable() {
    local tag=$1 branch=$2 from=$3
    if git ls-remote --exit-code --heads origin "$branch" >/dev/null; then
        die "$branch already exists - use merge instead"
    fi
    git fetch --quiet origin "+refs/heads/$from:refs/remotes/origin/$from" || die "no branch $from on origin"
    local base
    base=$(upstream_ref "$tag")
    # The fork's own commits are those in $from which aren't in any
    # upstream branch or tag. Merges are left out: upstream changes
    # come from the new base instead.
    local commits
    # shellcheck disable=SC2046
    commits=$(git rev-list --reverse --no-merges "origin/$from" --not $(git for-each-ref --format='%(refname)' refs/remotes/upstream refs/upstream-tags))
    [[ -n "$commits" ]] || die "found no fork commits in $from"
    start_work "$base"
    echo "Cherry-picking $(wc -l <<<"$commits") commits from $from onto $tag:"
    local c
    for c in $commits; do
        git log -1 --format='  %h %s' "$c"
        if ! git cherry-pick --allow-empty "$c" >/dev/null; then
            git rev-parse --quiet --verify CHERRY_PICK_HEAD >/dev/null || die "cherry-pick failed"
            local all="" x
            for x in $commits; do all+=" ${x:0:12}"; done
            conflict "Cherry-picking $(git log -1 --format='%h %s' "$c")" \
                "git checkout -b $branch $tag && git cherry-pick$all"
            git cherry-pick --abort
            exit 2
        fi
    done
}

[[ $# -ge 1 ]] || die "usage: $0 merge <upstream-ref> <branch> | new-stable <upstream-tag> <branch> <from-branch>"
mode=$1
shift
if [[ $(git rev-parse --is-shallow-repository) == true ]]; then
    die "this is a shallow clone - run: git fetch --unshallow origin"
fi
fetch_upstream
case $mode in
    merge)
        [[ $# -eq 2 ]] || die "usage: $0 merge <upstream-ref> <branch>"
        merge "$@"
        ;;
    new-stable)
        [[ $# -eq 3 ]] || die "usage: $0 new-stable <upstream-tag> <branch> <from-branch>"
        new_stable "$@"
        ;;
    *)
        die "unknown mode $mode"
        ;;
esac
