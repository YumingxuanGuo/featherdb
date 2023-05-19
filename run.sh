#!/usr/bin/env bash

set -euo pipefail

cargo build --bin feather_kv
cargo build --bin feather_db

for ID in a b c; do
    (./target/debug/feather_kv clusters/feather_kvs/kv-$ID/feather_kv.yaml 2>&1 | sed -e "s/\\(.*\\)/|kv-$ID|  \\1/g") &
done

for ID in a; do
    (./target/debug/feather_db clusters/feather_dbs/db-$ID/feather_db.yaml 2>&1 | sed -e "s/\\(.*\\)/|db-$ID|  \\1/g") &
done

exit_port() {
    for PORT in 9501 9701 9702 9703 9601 9602 9603; do
        lsof -i :$PORT | awk 'NR!=1 {print $2}' | xargs kill
    done
    kill $(jobs -p)
}

trap exit_port EXIT
wait < <(jobs -p)