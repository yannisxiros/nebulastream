#!/usr/bin/env bash
# grab-data.sh
# Download testdata files referenced by .md5 manifests from TU Berlin's tubcloud.

set -euo pipefail

# Default URL template for NebulaStream test data
# Template variables: {algo} and {hash}
DEFAULT_TEMPLATE='https://tubcloud.tu-berlin.de/s/28Tr2wTd73Ggeed/download?files={algo}_{hash}'
TEMPLATE=${NES_EXTERNAL_DATA_TEMPLATE:-$DEFAULT_TEMPLATE}

# Base path for testdata
TESTDATA_BASE="nes-systests/testdata/large"

log() { printf "[%s] %s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }
err() { log "ERROR: $*" >&2; }

# Search roots (default: base testdata dir if no args provided)
if [ $# -eq 0 ]; then
  ROOT_DIRS=("$TESTDATA_BASE")
else
  ROOT_DIRS=()
  for arg in "$@"; do
    if [ -d "$arg" ]; then
      ROOT_DIRS+=("$arg")
    elif [ -d "$TESTDATA_BASE/$arg" ]; then
      ROOT_DIRS+=("$TESTDATA_BASE/$arg")
    else
      err "Directory '$arg' not found (checked locally and under $TESTDATA_BASE)"
    fi
  done
fi

if [ ${#ROOT_DIRS[@]} -eq 0 ] && [ $# -gt 0 ]; then
  err "No valid directories found to search. Exiting."
  exit 1
fi


if ! command -v md5sum >/dev/null 2>&1; then
  err "md5sum is not installed. Please install coreutils."
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  err "curl is not installed. Please install curl."
  exit 1
fi

download_one() {
  local md5file="$1"
  local hash algorithm filename url tmpout

  hash=$(tr -d '\r\n \t' < "$md5file")
  if [[ -z "$hash" ]]; then
    err "Empty hash in $md5file, skipping"
    return 1
  fi

  algorithm="MD5"
  filename="${md5file%.md5}"
  url=$(printf '%s' "$TEMPLATE" | sed "s/{algo}/$algorithm/g; s/{hash}/$hash/g")
  tmpout="${filename}.part"

  # Skip if file already exists and matches hash
  if [[ -f "$filename" ]]; then
    local cur
    cur=$(md5sum "$filename" | awk '{print $1}')
    if [[ "$cur" == "$hash" ]]; then
      log "OK (exists): $filename"
      return 0
    fi
  fi

  log "Downloading $filename..."
  mkdir -p "$(dirname "$filename")"

  if curl -fL --retry 3 --retry-delay 2 -o "$tmpout" "$url"; then
    local got
    got=$(md5sum "$tmpout" | awk '{print $1}')
    if [[ "$got" == "$hash" ]]; then
      mv -f "$tmpout" "$filename"
      log "Verified: $filename"
      return 0
    else
      err "Checksum mismatch for $filename (got $got, expected $hash)"
      rm -f "$tmpout"
      return 1
    fi
  else
    err "Failed to download $url"
    rm -f "$tmpout"
    return 1
  fi
}

log "Starting download of testdata from: ${ROOT_DIRS[*]}"

# Find all .md5 files recursively within specified directories
for dir in "${ROOT_DIRS[@]}"; do
  while IFS= read -r md5; do
    download_one "$md5" || true
  done < <(find "$dir" -type f -name '*.md5')
done

log "Finished."
