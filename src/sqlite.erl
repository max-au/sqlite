%%%-------------------------------------------------------------------
%%% @copyright (C) 2023 Maxim Fedorov
%%% sqlite3 NIF bindings
%%% @doc
%%%
%%% @end
-module(sqlite).
-author("maximfca@gmail.com").

%% API
-export([open/1, open/2, close/1, status/1,
    query/2, query/3, prepare/2, prepare/3, info/1, describe/1, execute/2,
    monitor/1, demonitor/1, interrupt/1, system_info/0, get_last_insert_rowid/1
]).

%% NIF loading code
-on_load(init/0).
-nifs([sqlite_open_nif/2, sqlite_close_nif/1, sqlite_status_nif/1,
    sqlite_query_nif/3, sqlite_prepare_nif/3, sqlite_info_nif/1, sqlite_execute_nif/2,
    sqlite_monitor_nif/2, sqlite_describe_nif/1, sqlite_interrupt_nif/1,
    sqlite_system_info_nif/0, sqlite_get_last_insert_rowid_nif/1, sqlite_dirty_close_nif/0]).

-define(nif_stub, nif_stub_error(?LINE)).
nif_stub_error(Line) ->
    erlang:nif_error({nif_not_loaded,module, ?MODULE, line, Line}).

-type connection() :: reference().
%% SQLite connection resource reference.

-type connect_flag() :: memory | uri | shared.
%% SQLite connections flags specification
%%
%% Normally used to specify an in-memory database: `flags => [memory]'.
%%
%% Example of an in-memory database shared between multiple connections:
%% ```
%% Db1 = sqlite:open("file::mem:", #{flags => [shared, memory, uri]}),
%% Db2 = sqlite:open("file::mem:", #{flags => [shared, memory, uri]}),
%% '''
%% <ul>
%%   <li>`memory': enables `SQLITE_OPEN_MEMORY' flag</li>
%%   <li>`uri': enables `SQLITE_OPEN_URI' flag</li>
%%   <li>`shared': enables `SQLITE_OPEN_SHAREDCACHE' flag </li>
%% </ul>

-type connect_options() :: #{
    mode => read_only | read_write | read_write_create,
    flags => [connect_flag()],
    busy_timeout => non_neg_integer()
}.
%% SQLite connection options.
%%
%% <ul>
%%   <li>`mode': file open mode, the default is `read_write_create'</li>
%%   <li>`flags': SQLite connection flags specification</li>
%%   <li>`busy_timeout': specifies SQLite busy timeout. NOTE: using this setting may
%%     easily exhaust all ERTS Dirty I/O schedulers. Prefer WAL mode instead</li>
%% </ul>

-type lookaside_memory() :: #{
    used => integer(),
    max => integer(),
    hit => integer(),
    miss_size => integer(),
    miss_full => integer()
}.
%% SQLite connection lookaside memory usage
%%
%% <ul>
%%   <li>`used': the current number of lookaside memory slots currently checked out
%%     (current value of `SQLITE_DBSTATUS_LOOKASIDE_USED')</li>
%%   <li>`max': high watermark of `SQLITE_DBSTATUS_LOOKASIDE_USED'</li>
%%   <li>`hit': number of malloc attempts that were satisfied using lookaside memory
%%     (high watermark of `SQLITE_DBSTATUS_LOOKASIDE_HIT')</li>
%%   <li>`miss_size': the number malloc attempts that might have been satisfied using lookaside
%%     memory but failed due to the amount of memory requested being larger than the lookaside slot size
%%     (high watermark of `SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE')</li>
%%   <li>`miss_full': the number malloc attempts that might have been satisfied using lookaside memory
%%     but failed due to all lookaside memory already being in use
%%     (high watermark of `SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL')</li>
%% </ul>

-type pager_cache_memory() :: #{
    used => integer(),
    shared => integer(),
    hit => integer(),
    miss => integer(),
    write => integer(),
    spill => {Current :: integer(), Max :: integer()}
}.
%% SQLite connection pager cache memory usage
%%
%% <ul>
%%   <li>`used': the approximate number of bytes of heap memory used by all pager caches
%%     associated with the database connection (current value of `SQLITE_DBSTATUS_CACHE_USED')</li>
%%   <li>`shared': current value of `SQLITE_DBSTATUS_CACHE_USED_SHARED'</li>
%%   <li>`hit':  the number of pager cache hits that have occurred
%%     (current value of `SQLITE_DBSTATUS_CACHE_HIT')</li>
%%   <li>`miss': the number of pager cache misses that have occurred
%%     (current value of SQLITE_DBSTATUS_CACHE_MISS)</li>
%%   <li>`write': the number of dirty cache entries that have been written to disk
%%     (current value of `SQLITE_DBSTATUS_CACHE_WRITE')</li>
%%   <li>`spill': the number of dirty cache entries that have been written to disk in the middle
%%     of a transaction due to the page cache overflowing. Both values of `SQLITE_DBSTATUS_CACHE_SPILL'</li>
%% </ul>


-type connection_status() :: #{
    lookaside_memory => lookaside_memory(),
    pager_cache_memory => pager_cache_memory(),
    schema => integer(),
    statement => integer(),
    deferred_fks => integer()
}.
%% SQLite connection status
%%
%% <ul>
%%   <li>`schema': the approximate number of bytes of heap memory used to store the schema for
%%     all databases associated with the connection (current value of `SQLITE_DBSTATUS_SCHEMA_USED')</li>
%%   <li>`statement': the approximate number of bytes of heap and lookaside memory used by all
%%     prepared statements associated with the database connection
%%     (current value of `SQLITE_DBSTATUS_STMT_USED')</li>
%%   <li>`deferred_fks': returns zero for the current value if and only if all foreign key
%%     constraints (deferred or immediate) have been resolved (current value of `SQLITE_DBSTATUS_DEFERRED_FKS')</li>
%% </ul>

-type prepared_statement() :: reference().
%% Prepared statement reference.

-type column_type() :: integer | float | binary.
%% SQLite column type mapped to an Erlang type.

-type column_name() :: binary().
%% Column name

-type parameter() :: integer() | float() | binary() | string() | iolist().
%% Erlang type allowed to be used in SQLite bindings.

-type row() :: tuple().

-type prepare_options() :: #{
    persistent => boolean(),
    no_vtab => boolean()
}.
%% Prepared statement creation flags
%%
%% <ul>
%%   <li>`persistent': The SQLITE_PREPARE_PERSISTENT flag is a hint to the query planner that
%%      the prepared statement will be retained for a long time and probably reused many times..</li>
%%   <li>`no_vtab': The SQLITE_PREPARE_NO_VTAB flag causes the SQL compiler to return an error
%%      (error code SQLITE_ERROR) if the statement uses any virtual tables. .</li>
%% </ul>

-type statement_info() :: #{
    fullscan_step => integer(),
    sort => integer(),
    autoindex => integer(),
    vm_step => integer(),
    reprepare => integer(),
    run => integer(),
    filter_miss => integer(),
    filter_hit => integer(),
    memory_used => integer()
}.
%% Prepared statement statistics
%%
%% Performance counters for SQLite statement. See `sqlite3_stmt_status'
%% function in SQLite reference.

-type system_info() :: #{
    memory_used => {Cur :: integer(), Max :: integer()},
    page_cache => {Size :: integer(), Used :: integer(), Max :: integer(), Overflow :: integer(), OverflowMax :: integer()},
    malloc => {Size :: integer(), Count :: integer(), Max :: integer()}
}.
%% Status information about the performance of SQLite
%%
%% See `sqlite3_status' in the SQLite reference for more details about
%% exported statistics.

%% @equiv open(FileName, #{})
-spec open(file:filename_all()) -> connection().
open(FileName) ->
    open(FileName, #{}).

%% @doc Open the connection to a specified SQLite database.
-spec open(file:filename_all(), Options :: connect_options()) -> connection().
open(FileName, Options) ->
    case sqlite_open_nif(FileName, Options) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [FileName, Options], [{error_info, #{cause => ExtErr}}]);
        Connection ->
            Connection
    end.

%% @doc Close the connection.
%%
%% Ensures that all prepared statements are deallocated. Releases
%% underlying database file.
%% Throws `badarg' if the connection is already closed.
-spec close(connection()) -> ok.
close(Connection) ->
    case sqlite_close_nif(Connection) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Connection], [{error_info, #{cause => ExtErr}}]);
        ok ->
            ok
    end.

%% @doc Returns connection statistics.
%%
%% Throws `badarg' if the connection is closed.
-spec status(connection()) -> connection_status().
status(Connection) ->
    case sqlite_status_nif(Connection) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Connection], [{error_info, #{cause => ExtErr}}]);
        Status ->
            Status
    end.

%% @equiv query(Connection, Query, []).
-spec query(connection(), iodata()) -> [row()].
query(Connection, Query) ->
    query(Connection, Query, []).

%% @doc Runs an SQL query using specified connection
%%
%% Returns a list of database rows, or an empty list if no rows are expected
%% to be returned (e.g. `CREATE TABLE' statement).
%%
%% Query may contain placeholders, e.g. `?', `?1', `?2'. Number of placeholders
%% must match the length of the parameters list. Binging function depends on
%% the passed parameter type:
%% <ul>
%%   <li>`undefined': </li>
%%   <li>`binary()': </li>
%%   <li>`integer()': </li>
%%   <li>`float()': </li>
%% </ul>
%%
%%
%% Throws `badarg' if connection is closed.
-spec query(connection(), iodata(), [parameter()]) -> [row()].
query(Connection, Query, Parameter) ->
    case sqlite_query_nif(Connection, Query, Parameter) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Connection, Query, Parameter], [{error_info, #{cause => ExtErr}}]);
        Result when is_list(Result) ->
            lists:reverse(Result)
    end.

%% @equiv prepare(Connection, Query, #{}).
-spec prepare(connection(), iodata()) -> prepared_statement().
prepare(Connection, Query) ->
    prepare(Connection, Query, #{}).

%% @doc Creates a prepared statement.
%%
%% Use `execute/2' to run the statement.
%% Note that `close/1' invalidates all prepared statements created
%% using the specified connection.
%%
%% By default, the prepared statement is not persistent. Note that
%% if a single statement is going to be reused many times, it may
%% prove useful to pass `persistent' option set to `true'.
%%
%% Running a single `query/3' once is more efficient than preparing
%% a statement, executing it once and discarding.
%%
%% Throws `badarg' if the connection is closed.
-spec prepare(connection(), iodata(), prepare_options()) -> prepared_statement().
prepare(Connection, Query, Options) ->
    case sqlite_prepare_nif(Connection, Query, Options) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Connection, Query, Options], [{error_info, #{cause => ExtErr}}]);
        Prepared ->
            Prepared
    end.

%% @doc Returns column names and types for the prepared statement.
%%
%% Throws `badarg' if the connection is closed.
-spec describe(prepared_statement()) -> [{column_name(), column_type()}].
describe(Prepared) ->
    case sqlite_describe_nif(Prepared) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Prepared], [{error_info, #{cause => ExtErr}}]);
        Description ->
            Description
    end.

%% @doc Returns runtime statistics for the prepared statement.
%%
%% Throws `badarg' if the connection is closed.
-spec info(prepared_statement()) -> statement_info().
info(Prepared) ->
    case sqlite_info_nif(Prepared) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Prepared], [{error_info, #{cause => ExtErr}}]);
        Description ->
            Description
    end.

%% @doc Runs the prepared statement with parameters bound.
%%
%% See `query/3' for types and bindings details.
%% Throws `badarg' if the connection is closed.
-spec execute(prepared_statement(), [parameter()]) -> [row()].
execute(Prepared, Parameters) ->
    case sqlite_execute_nif(Prepared, Parameters) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Prepared, Parameters], [{error_info, #{cause => ExtErr}}]);
        Result when is_list(Result) ->
            lists:reverse(Result)
    end.

%% @doc EXPERIMENTAL: monitor updates happening through the connection.
%%
%% This function is intended for debugging INSERT, UPDATE and DELETE
%% operations. The API is experimental and may change in the future
%% without prior notice.
%%
%% Only one process may monitor a connection. Subsequent monitor calls
%% replace the previously set process.
%%
%% Upon successful completion, calling process may start receiving
%% messages of the following format:
%% ```
%% {Ref, Op, Database, Table, RowID}
%% Example:
%% {#Ref<0.1954006965.2226257955.79689>, insert, <<"main">>, <<"table">>, 123}
%% '''
%% `Ref' is the reference returned by `monitor/1' call, `Op' is one of
%% `insert', `update' or `delete' operations, and `RowID' is the SQLite
%% ROWID.
-spec monitor(connection()) -> reference().
monitor(Connection) ->
    case sqlite_monitor_nif(Connection, self()) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Connection], [{error_info, #{cause => ExtErr}}]);
        Ref ->
            Ref
    end.

%% @doc Stops monitoring previously monitored connection.
%%
%% Does not flush messages that are already in transit.
-spec demonitor(reference()) -> ok.
demonitor(Ref) ->
    case sqlite_monitor_nif(Ref, undefined) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Ref], [{error_info, #{cause => ExtErr}}]);
        ok ->
            ok
    end.

%% @doc Interrupts the query running on this connection.
%%
%% WARNING: this function is unsafe to use concurrently with
%% closing the connection.
%%
%% Throws `badarg' if the connection is closed.
-spec interrupt(connection()) -> ok.
interrupt(Connection) ->
    sqlite_interrupt_nif(Connection).

%% @doc Returns SQLite system information (memory usage, page cache usage, allocation statistics)
-spec system_info() -> system_info().
system_info() ->
    sqlite_system_info_nif().

%% @doc Usually returns the ROWID of the most recent successful INSERT
%% into a rowid table or virtual table on database connection.
%%
%% See `sqlite3_last_insert_rowid' in the SQLite reference.
-spec get_last_insert_rowid(connection()) -> integer().
get_last_insert_rowid(Connection) ->
    sqlite_get_last_insert_rowid_nif(Connection).

%%-------------------------------------------------------------------
%% NIF stubs

sqlite_open_nif(_FileName, _Options) ->
    ?nif_stub.

sqlite_close_nif(_Connection) ->
    ?nif_stub.

sqlite_status_nif(_Connection) ->
    ?nif_stub.

sqlite_query_nif(_Connection, _Query, _Parameters) ->
    ?nif_stub.

sqlite_prepare_nif(_Connection, _Query, _Options) ->
    ?nif_stub.

sqlite_info_nif(_Prepared) ->
    ?nif_stub.

sqlite_describe_nif(_Prepared) ->
    ?nif_stub.

sqlite_execute_nif(_Prepared, _Parameters) ->
    ?nif_stub.

sqlite_monitor_nif(_Connection, _Pid) ->
    ?nif_stub.

sqlite_interrupt_nif(_Connection) ->
    ?nif_stub.

sqlite_system_info_nif() ->
    ?nif_stub.

sqlite_get_last_insert_rowid_nif(_Connection) ->
    ?nif_stub.

sqlite_dirty_close_nif() ->
    ?nif_stub.

%%-------------------------------------------------------------------
%% Internal implementation

%% The delayed deallocation process is needed when all connection
%% references were Garbage Collected, but the connection was not
%% closed. It is unsafe to close the connection in the GC callback
%% code, because it could lock the scheduler for an arbitrary amount
%% of time, and may lock the entire VM as it relies on schedulers being
%% responsive.
%% So the trick is to have a process that will collect such orphan
%% connections and run delayed deallocation in a dirty I/O scheduler.
%% There is no other purpose of this process.
%% If the module gets purged, this process is stopped forcibly,
%% causing NIF to fall back to closing connection in the GC callback,
%% printing a warning to stderr.
delayed_dealloc() ->
    receive
        undefined ->
            sqlite_dirty_close_nif(),
            delayed_dealloc()
    end.

init() ->
    Purger = erlang:spawn(fun delayed_dealloc/0),
    NifAlloc = application:get_env(sqlite, enif_alloc, true),
    Priv = code:priv_dir(sqlite),
    SoFile = filename:join(Priv, ?MODULE_STRING),
    case erlang:load_nif(SoFile, {Purger, NifAlloc}) of
        ok ->
            ok;
        {error, {load, "Library load-call unsuccessful (-1)."}} ->
            {error, {load, "Failed to configure memory allocation functions"}};
        {error, {load, "Library load-call unsuccessful (-2)."}} ->
            {error, {load, "Linked sqlite version does not support multithreading"}};
        {error, {load, "Library load-call unsuccessful (-3)."}} ->
            {error, {load, "Failed to initialize sqlite"}};
        {error, {load, "Library load-call unsuccessful (-4)."}} ->
            {error, {load, "Out of memory creating resources"}};
        {error, {load, "Library load-call unsuccessful (-6)."}} ->
            {error, {load, "Invalid NIF load_info passed, check if delayed dealloc process is alive"}};
        Other ->
            Other
    end.
