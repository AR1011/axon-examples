#!/usr/bin/env bash
# JournalSeal.cfg must pass, and each mutations/JournalSeal-*.cfg must end with an invariant violated.
# Run from this directory; TLA_JAR names tla2tools.jar (default ~/tla/tla2tools.jar).
set -u
jar=${TLA_JAR:-$HOME/tla/tla2tools.jar}
tlc() { java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto -deadlock -metadir "$(mktemp -d)" -config "$1" JournalSeal.tla 2>&1; }
failed=0
out=$(tlc JournalSeal.cfg)
if grep -q "Model checking completed. No error has been found" <<<"$out"; then
    # The last count is the total; those before it are TLC's progress reports, one a minute.
    states=$(grep -oE '[0-9,]+ distinct states found' <<<"$out" | tail -n 1)
    echo "JournalSeal.cfg: passes, $states, $(sed -nE 's/^Finished in (.*) at .*/in \1/p' <<<"$out")"
else
    echo "JournalSeal.cfg: FAILS"; echo "$out" | tail -n 40; failed=1
fi
for cfg in mutations/JournalSeal-*.cfg; do
    out=$(tlc "$cfg")
    if v=$(grep -m1 -E "^Error: Invariant [A-Za-z]+ is violated" <<<"$out"); then
        echo "$cfg: fails as it must: ${v#Error: }"
    else
        echo "$cfg: NOT VIOLATED"; echo "$out" | tail -n 20; failed=1
    fi
done
exit $failed
