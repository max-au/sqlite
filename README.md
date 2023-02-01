# sqlite
sqlite3 NIF implementation.

## Not implemented
* reduce copy-paste in the NIF code
* fix format_error to print useful messages in the shell
* type bindings TEXT <-> BLOB
* test for INTERRUPT (may crash VM when called in parallel with close/1)
* performance (benchmarking) - especially in the memory allocation
* performance - concurrent access to the same connection/statement (locking efficiency)
* performance - see what can run on normal scheduler (yielding?)
* memory leaks tests
* better/proper monitoring for ROWID changes
* OS support (Linux, MacOS, Windows, FreeBSD)
* message-based asynchronous APIs (instead of busy wait)
* alphanumeric identifiers (non-positional) for bindings
* document how to format EEP-54 errors 

Extra Features
* Online Backup support
* Snapshot support
* Commit and Rollback hooks support
* Database serialisation (sqlite3_serialize)
* Explicit Step/Bind APIs
* More type conversions

## Cleanup
* cleanup code from "memory assertions" and test errors (out of memory etc)
* test enif_alloc/malloc performance
* POTENTIAL ERTS BUG: when the process holding RESOURCE reference exits, no destructor is called


## Extended error structure (cause)


## Erlang to sqlite data type mapping
Following primitive types in Erlang are mapped to corresponding sqlite types:
 * binary() <-> sqlite3 blob
 * string() <-> sqlite3 text
 * integer() <-> integer
 * float() <-> float
 * undefined <-> null

Potentially, in the future:
 * atom() <-> text?
 * map() <-> JSON?

