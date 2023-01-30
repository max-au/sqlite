# sqlite
sqlite3 NIF implementation.

## Not implemented
* Correct dirty I/O behaviour (not blocking GC when collecting sqlite connection)
* performance (benchmarking)
* Types mapping
* Concurrency tests
* Thread safety of "connection" and "statement" objects
* Yielding
* Online Backup support
* Snapshot support
* Commit and Rollback hooks support
* Memory leaks tests
* database serialisation (sqlite3_serialize)

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

