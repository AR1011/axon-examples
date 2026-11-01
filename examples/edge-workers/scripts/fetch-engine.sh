#!/usr/bin/env bash
# Fetches the StarlingMonkey runtime (SpiderMonkey compiled to wasm) the real engine runs, pinned by sha256,
# into examples/edge-workers/engine/ (not committed: 10 MB). Needs curl and sha256sum or shasum.
set -euo pipefail
cd "$(dirname "$0")/.."
REL=https://github.com/bytecodealliance/StarlingMonkey/releases/download/starlingmonkey-v0.3.0
mkdir -p engine
fetch() {
  local name=$1 want=$2 got
  if [ ! -f "engine/$name" ]; then curl -fsSL --retry 3 -o "engine/$name.part" "$REL/$name" && mv "engine/$name.part" "engine/$name"; fi
  got=$( (sha256sum "engine/$name" 2>/dev/null || shasum -a 256 "engine/$name") | cut -d' ' -f1)
  [ "$got" = "$want" ] || { echo "engine/$name: sha256 $got, expected $want" >&2; rm -f "engine/$name"; exit 1; }
  echo "engine/$name ok"
}
# The core module Wizer snapshots at deploy, and the adapter that makes the snapshot a WASI 0.2 component.
fetch starling-raw.wasm f080ce209a6423fdf3a59c5b28a3559baf4044f92799516c042edbfa3e718b41
fetch preview1-adapter.wasm 988224282d03fe0bd3c25b62344aef265b1a8e1f5804bdbc34d6442507fabcc0
