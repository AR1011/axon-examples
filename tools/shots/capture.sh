#!/usr/bin/env bash
# Runs commands on the box and saves what they print, for a terminal figure of tools/figures/figures.py.
#   tools/shots/capture.sh <box-dir> <out.txt> [--show 'as shown'] '<command>' [[--show '..'] '<command>']..
# Each command runs with bash in ~/axon/<box-dir> on $BOX (default build-1), stdout and stderr together. The file gets
# "$ <command>" (or the --show text, for a command whose real form carries ports or paths of the box) and then its
# output, with colour codes taken out. A command that fails stops the capture, and nothing is written.
set -euo pipefail
box=${BOX:-build-1}
dir=${1:?usage: capture.sh <box-dir> <out.txt> [--show text] command..} out=${2:?out file}
shift 2
case $dir in */*|.*) echo "box-dir is a plain name" >&2; exit 2 ;; esac
tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
show=
while [ $# -gt 0 ]; do
  if [ "$1" = --show ]; then show=$2; shift 2; continue; fi
  echo "\$ ${show:-$1}" >>"$tmp"
  # shellcheck disable=SC2029 # the command is meant to expand on the box
  if ! ssh "$box" "cd ~/axon/$dir && bash -c $(printf %q "$1")" 2>&1 | sed -e $'s/\x1b\\[[0-9;?]*[A-Za-z]//g' >>"$tmp"; then
    echo "capture.sh: the command failed: $1" >&2; tail -n 5 "$tmp" >&2; exit 1
  fi
  show=
  shift
done
mkdir -p "$(dirname "$out")"
mv "$tmp" "$out"
trap - EXIT
echo "wrote $out"
