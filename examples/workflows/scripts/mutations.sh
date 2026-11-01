#!/usr/bin/env bash
# Each weakened rule of the journal (Mutation in src/journal/core.rs) must fail the simulation on some seed, and
# the journal's World stories named beside it (tests/world.rs), and each weakened rule of a run or a node
# (RUN_MUTATION) the tests named beside it: embedded tests in tests/run.rs, World stories in tests/world_runs.rs.
# Run from the workspace root. SEEDS=a..b narrows or widens the seeds (500 for the simulation and 64 for the stories
# by default).
set -u
failed=0
for m in skip_fence no_fence_sync ack_on_one read_one_replica no_carry; do
    out=$(JOURNAL_MUTATION=$m cargo test -q -p workflows --test journal_sim every_seed 2>&1)
    if [ $? -eq 0 ]; then
        echo "$m: PASSED, the simulation does not catch it"
        failed=1
    else
        echo "$m: fails: $(grep -m1 -E '^seed [0-9]+ failed' <<<"$out" | cut -c1-200)"
    fi
done
# The journal's World stories (tests/world.rs) that catch a weakened rule. The design names more: (b) and (m) for
# skip_fence and (c) for ack_on_one, but those stories pass with them (README, Mutations): only the simulation and
# TLC catch them.
for m in read_one_replica:replicas_crash_one_after_another_and_are_repaired; do
    IFS=: read -r mutation test <<<"$m"
    out=$(JOURNAL_MUTATION=$mutation cargo test -q -p workflows --all-features --test world "$test" -- --exact 2>&1)
    if [ $? -eq 0 ]; then
        echo "$mutation: PASSED, $test does not catch it"
        failed=1
    else
        why=$(grep -m1 -E 'violation|I[0-9]+|panicked at' <<<"$out" | cut -c1-200)
        echo "$mutation: $test fails: $why"
    fi
done
# mutation:test file:test name
for m in number_at_poll:run:all_order \
    passivate_busy:run:an_attempt_in_flight_keeps_the_run \
    passivate_busy:world_runs:a_an_engine_process_crashes_mid_step \
    no_eager:world_runs:a_an_engine_process_crashes_mid_step \
    no_reconciler:world_runs:e_the_whole_cluster_crashes_and_the_reconciler_finishes_the_runs \
    request_until:world_runs:a_an_engine_process_crashes_mid_step \
    no_signal_dedup:world_runs:h_signals_and_cancels_race_passivation \
    no_input_check:world_runs:g_a_double_start_from_two_callers_through_an_engine_crash \
    drain_abandons:world_runs:k_a_worker_crashes_then_a_worker_drains; do
    IFS=: read -r mutation file test <<<"$m"
    out=$(RUN_MUTATION=$mutation cargo test -q -p workflows --all-features --test "$file" "$test" -- --exact 2>&1)
    if [ $? -eq 0 ]; then
        echo "$mutation: PASSED, $test does not catch it"
        failed=1
    else
        why=$(grep -m1 'message: ' <<<"$out" || grep -m1 -A1 'panicked at' <<<"$out" | tail -n 1)
        why=$(sed -E 's/.*message: //' <<<"$why" | cut -c1-200)
        echo "$mutation: $test fails: $why"
    fi
done
exit $failed
