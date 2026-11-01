#!/usr/bin/env bash
# The test targets of edge-workers in two parts for CI, as `cargo test` arguments: `first` is the files named here,
# `rest` is the library, the binaries and every other file in tests/, so a new file runs without being listed.
set -euo pipefail
first="admin gateway cluster"
cd "$(dirname "$0")/../tests"
case ${1:-} in
    first) for name in $first; do
               [ -f "$name.rs" ] || { echo "no tests/$name.rs" >&2; exit 1; }
               printf -- '--test %s ' "$name"
           done ;;
    rest)  printf -- '--lib --bins '
           for file in *.rs; do
               case " $first " in *" ${file%.rs} "*) ;; *) printf -- '--test %s ' "${file%.rs}" ;; esac
           done ;;
    *)     sed -n '2,3p' "$0" >&2; exit 2 ;;
esac
