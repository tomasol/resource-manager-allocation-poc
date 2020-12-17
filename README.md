# Prototype of new DB structure optimized for allocating of new resources

## Motivation

Prototype of allocating resources using different schema approach.
See migrations folder for the actual SQL schema.

Key differences:
* resource properties are JSONB column instead of separate table
* resource pools have version column for optimistic locking

### Shortcomings of the prototype
* No db pooling currently implemented
* Allocation strategy - only IPv4 is added to DB and used for resource allocation
* Pool properties are hardcoded in tests to: {"address": "10.0.0.0","prefix": 8}
* Resource states not supported: on bench, free - present resource == claimed

### Benefits
* No long running transactions
* No serializable isolation
* One pool allocation cannot degrade performance of other pool allocations
* Fast bulk insertion of resources using a single INSERT statement
* No need to check for resource duplicates - use UNIQUE constraint
* No performance degradation when resources contains 10k+ rows

### Future exploration
Tight wasmer integration


## Running
Export following env.vars:
```sh
export WASMER_BIN=~/.wasmer/bin/wasmer
export WASMER_JS=~/.wasmer/globals/wapm_packages/_/quickjs@0.0.3/build/qjs.wasm
export DB_PARAMS="host=localhost user=postgres password=postgres dbname=rm-poc"
```

To run all tests, use:
```sh
cargo test --release
```

To run a single test, and to modify certain env.vars, use:
```sh
NUMBER_OF_THREADS=10 VERIFY_RESOURCES=1 ROW_COUNT=1 RUST_BACKTRACE=FULL RUST_LOG=info \
cargo test --release -- --nocapture tests::parallel_allocation
```
