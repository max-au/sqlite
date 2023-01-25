# sqlite
sqlite3 NIF implementation.

## Not implemented
* Types mapping
* Thread safety of "connection" and "statement" objects
* Yielding and dirty NIFs
* Online Backup support
* Snapshot support
* Commit and Rollback hooks support
* Memory leaks tests

## Build
-----

    $ rebar3 compile

## Erlang to sqlite data type mapping
Following primitive types in Erlang are mapped to corresponding sqlite types:
 * binary() <-> sqlite3 blob
 * string() <-> sqlite3 text
 * integer() <-> integer
 * float() <-> float
 * undefined <-> null

## TODO
Before 1.0 release, following features must be explored:
 * memory allocators done through BEAM (for accounting purposes)
 * directories (data and temp directory settings)
 * mutex methods (for lock counting purposes)
 * better testing for extended error information (not just badarg)

## Failing to load

If the NIF fails with "Library load-call unsuccessful":
 * -1 sqlite3 was not built with thread safety turned on
 * -2 connection resource creation was unsuccessful
 * -3 statement resource creation failed