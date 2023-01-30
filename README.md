# sqlite
sqlite3 NIF implementation.

## Not implemented
* Types mapping
* performance (benchmarking) - especially in the memory allocation
* performance - concurrent access to the same connection/statement
* performance - see what can run on normal scheduler (yielding?)
* memory leaks tests

Extra Features
* Online Backup support
* Snapshot support
* Commit and Rollback hooks support
* Database serialisation (sqlite3_serialize)

## Cleanup
* debug settings from c_src Makefile
* sqlite amalgamation (provide with a NIF)
* debug scripts (place somewhere?)


## Extended error structure (cause)

```erlang
-type cause() :: #{
    general => binary(),        %% sub-operation that failed "error opening connection"
    detail => binary(),         %% detailed human-readable error, e.g. "missing permissions"
    position => integer()       %% only set for preparing statements or, binding arguments, - column
}.
```

## Build
-----

    $ rebar3 compile

## Erlang to sqlite data type mapping
Following primitive types in Erlang are mapped to corresponding sqlite types:
 * binary() <-> sqlite3 blob
 * string() <-> sqlite3 text
 * atom() <-> sqlite text
 * integer() <-> integer
 * float() <-> float
 * undefined <-> null
 * map() <-> JSON

## Failing to load

If the NIF fails with "Library load-call unsuccessful":
* -1 fails to set memory functions
* -2 fails to multithreading mode
* -3 failed to initialise
* -4 connection resource creation was unsuccessful
* -5 statement resource creation failed

