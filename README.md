# Prototype of new DB structure optimized for allocating of new resources

## Motivation

Prototype of allocating resources using different schema approach.
See migrations folder for the actual SQL schema.

Key differences:
* resource properties are JSONB column instead of separate table
* resource pools have version column for optimistic locking

### PoC goals
* Improve performance of `claimResources(resourceCount:100)`
* There should be no performance degradation of `claimResources(100)` on
newly created pool when there are 30k unrelated resources 
* Parallel acquisition of distinct pools should not block

### Out of scope
* Tight wasmer integration
* tx isolation vs mutex vs redis locking - currently just using optimistic
locking and assuming that parallel acquisition on the same pool will rarely
happen or will be batched by an external system
* db pooling
* Server, RPCs, thread pools etc.
* Allocation strategies - only IPv4 is used for benchmarking
* Pool properties are hardcoded in tests to: `{"address": "10.0.0.0","prefix": 8}`
* Resource states not supported: `on bench`

### Results
* One pool allocation does not degrade performance of other pool allocations
* Fast bulk insertion of resources using a single INSERT statement: 
`claimResources(100)` takes ~70ms, out of which 55ms is wasmer
* No need to check for resource duplicates - use UNIQUE constraint
* No performance degradation when the DB contains 30k of unrelated resources

## Running
Create database `rm-poc` according to the  [migrations](migrations) folder.

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
NUMBER_OF_THREADS=10 VERIFY_RESOURCES=1 ROW_COUNT=1 RUST_BACKTRACE=1 RUST_LOG=info \
cargo test --release -- --nocapture tests::parallel_allocation
```
