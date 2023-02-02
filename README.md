# sqlite
sqlite3 NIF bindings for Erlang.

See `sqlite:query/3` for the data type conversion details. Current version
assumes UTF8 encoding for all TEXT columns.

All functions that may affect scheduling run in the dirty I/O scheduler.
Keep an eye on the dirty scheduler utilisation, and bump the number when
necessary (using `+SDio` argument).

## Build
Use `rebar3 compile` to build the application using included SQLite3
amalgamation. Specify `USE_SYSTEM_SQLITE` environment variable to
build against sqlite version provided by the system (tested with
sqlite 3.37.2 and newer).

sqlite3 amalgamation build flags are accepted through `CFLAGS`, e.g.
`CFLAGS="-DSQLITE_USE_URI" rebar3 compile`.

For a debug build, specify `DEBUG=1` environment variable. Both
NIF bindings and sqlite amalgamation files are affected by this flag. Remember
to run `rebar3 clean` when changing build flavour.

Requires OTP 24 and above (older versions do not support extended error
specification per EEP-54). Find a list of supported Operating Systems in
the corresponding section.

## Quick start
The easiest way to try it out is to run `rebar3 shell` within the cloned
project folder.

```
1> Conn = sqlite:open("mem", #{flags => [memory]}). %% open an in-memory database
#Ref<0.1238832725.87949353.240438>
2> sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY AUTOINCREMENT, val INTEGER)").
[]
3> Prepared = sqlite:prepare(Conn, "INSERT INTO kv (val) VALUES (?1)", #{persistent => true}).                                             
#Ref<0.1238832725.87949353.240439>
4> [sqlite:execute(Prepared, [Seq]) || Seq <- lists:seq(1, 100)].
[[],[],[],[],[],[]|...]
5> sqlite:query(Conn, "SELECT * FROM kv").
[{1,1}, {2,2}, <...>
```

## Configuration
Due to significant performance difference observed for in-memory databases,
there is an option to fall back to sqlite built-in memory allocation
functions. Set `enif_malloc` configuration variable to `false` (the default
is `true`) to let sqlite use built-in allocators instead of the recommended
`enif_alloc`. As a consequence, memory allocated by sqlite3 will no longer
be visible via usual Erlang functions. For example `erlang:memory/1` won't be
able to report sqlite3 allocations). Use this option only when you need that
last bit of performance.

## Profiling with Linux perf

Ensure ERTS with frame pointers enabled (`frmptr` flavour) is available.

```bash
export ERL_FLAGS="+JPperf true -emu_type frmptr"
export CFLAGS="-fno-omit-frame-pointer""

# run the benchmark test case
rebar3 ct --suite sqlite_QUITE --case benchmark_prepared

# take 10 second of perf profiling
perf record -g -p `pidof rebar3` -F 500 -- sleep 10

# convert to a flame graph
perf script | ./stackcollapse-perf.pl > out.perf-folded
./flamegraph.pl out.perf-folded > perf.svg
```

## Operating system support
Initial version has been tested on Linux (Ubuntu), Mac OS, FreeBSD 13 and
Windows 10.

### Mac OS
If you see Apple Silicon (ARM64) build failing with this diagnostic:
```
Undefined symbols for architecture arm64:
  "_enif_alloc", referenced from:
```
Clear the `LDFLAGS` variable with `unset LDFLAGS` and try again.

### Windows
Windows build, with `rebar3` as an escript, assuming Ericsson official installation
into `C:\Program Files\Erlang OTP`. If you have it elsewhere, set `ERTS_INCLUDE_DIR`
environment variable appropriately, to the `usr/include` subfolder of your installation.

Build requires `x64 Native Tools Command Prompt`. Tested with VS 2022. Ensure
to see the slogan: `[vcvarsall.bat] Environment initialized for: 'x64'`

```
# set up PATH to escript/erl
set PATH=%PATH%;C:\Program Files\Erlang OTP\bin
# download or build rebar3 (assuming it is now in the current directory)
# use rebar3 to build sqlite and run Common Test
escript.exe rebar3 ct
```

## Anticipated changes beyond 1.0
These features did not make it into 1.0, but are useful and may be implemented
in the following releases:
* online backup support
* alphanumeric identifiers (non-positional) for bindings in prepared statements
* support for atom() and map() type conversion
* asynchronous `query` and `execute` APIs (with message exchange on return)
* sqlite hooks support (commit, preupdate, rollback, update, wal)
* snapshot support
* large blob handling
* sqlite vfs support (with Erlang distribution)
* database serialisation (sqlite3_serialize)
* improved performance for concurrent access to the same connection/statement,
  running on a normal scheduler and yielding

## TODO: LIST OF THINGS TO BE DONE BEFORE RELEASE
* setup CI on GitHub
* rename to sqlite3
* possible ERTS bug: when the process holding RESOURCE reference exits, destructor is not called
