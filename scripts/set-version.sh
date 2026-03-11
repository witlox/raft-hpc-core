#!/usr/bin/env bash
# set-version.sh — Reads the base version from workspace Cargo.toml,
# replaces the patch component (`.0`) with the git commit count,
# and patches all version locations in the repo.
#
# Usage:
#   ./scripts/set-version.sh          # patches files in-place
#   ./scripts/set-version.sh --dry-run # prints the computed version only
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# 1. Read base version from the single source of truth
BASE_VERSION=$(sed -n 's/^version = "\(.*\)"/\1/p' "$REPO_ROOT/Cargo.toml" | head -1)
if [ -z "$BASE_VERSION" ]; then
    echo "error: could not read version from Cargo.toml" >&2
    exit 1
fi

# 2. Extract year.major prefix (everything before the last dot)
PREFIX="${BASE_VERSION%.*}"

# 3. Compute commit count
COMMIT_COUNT=$(git -C "$REPO_ROOT" rev-list --count HEAD)

# 4. Assemble release version
RELEASE_VERSION="${PREFIX}.${COMMIT_COUNT}"

if [ "${1:-}" = "--dry-run" ]; then
    echo "$RELEASE_VERSION"
    exit 0
fi

echo "Setting version: $BASE_VERSION -> $RELEASE_VERSION"

# 5. Patch all version locations
# Cargo.toml (the source)
sed -i.bak "s/^version = \"${BASE_VERSION}\"/version = \"${RELEASE_VERSION}\"/" \
    "$REPO_ROOT/Cargo.toml"

# rm-replay (standalone workspace) - if exists
if [ -f "$REPO_ROOT/tools/rm-replay/Cargo.toml" ]; then
    sed -i.bak "s/^version = \"${BASE_VERSION}\"/version = \"${RELEASE_VERSION}\"/" \
        "$REPO_ROOT/tools/rm-replay/Cargo.toml"
fi

# Python SDK - if exists
if [ -f "$REPO_ROOT/sdk/python/pyproject.toml" ]; then
    sed -i.bak "s/^version = \"${BASE_VERSION}\"/version = \"${RELEASE_VERSION}\"/" \
        "$REPO_ROOT/sdk/python/pyproject.toml"
fi

# Clean up sed backup files
find "$REPO_ROOT" -name '*.bak' -delete

# 6. Update Cargo.lock
cargo generate-lockfile --manifest-path "$REPO_ROOT/Cargo.toml" 2>/dev/null || true

echo "Done: all versions set to $RELEASE_VERSION"
