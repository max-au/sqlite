%%% @copyright (C) 2023 Maxim Fedorov
%%% @doc
%%% sqlite3 NIF bindings for Erlang.
%%%
%%% See {@link query/3} for the data type conversion rules.
%%%
%%% All APIs of this module provide extended error information for the shell.
%%% This information is also accessible programmatically using `erl_error'.
%%% ```
%%% try
%%%    sqlite:prepare(sqlite:open(""), "INSERT INTO kv (key, val) VALU1ES (2, 2)")
%%% catch
%%%    Class:Reason:Stack ->
%%%        Formatted = erl_error:format_exception(Class, Reason, Stack),
%%%        io:format(lists:flatten(Formatted))
%%% end.
%%% '''
%%% @end
%%%
%%% Extended errors: for SQL errors, "general" contains the error code text,
%%%  and the detailed text is tied to an argument.
%%%
-module(sqlite).
-author("maximfca@gmail.com").

%% API
-export([open/1, open/2, close/1, status/1, reset/1, clear/1, finish/1,
    query/2, query/3, prepare/2, prepare/3, bind/2, step/1, step/2, info/1, describe/1, execute/2,
    monitor/1, demonitor/1, interrupt/1, system_info/0, get_last_insert_rowid/1,
    backup/2, backup/3
]).

%% Internal export for erl_error formatter
-export([format_error/2]).

%% NIF loading code
-on_load(init/0).
-nifs([sqlite_open_nif/2, sqlite_close_nif/1, sqlite_status_nif/1, sqlite_reset_nif/1, sqlite_clear_nif/1,
    sqlite_query_nif/3, sqlite_prepare_nif/3, sqlite_bind_nif/2, sqlite_info_nif/1, sqlite_execute_nif/2,
    sqlite_step_nif/2, sqlite_monitor_nif/2, sqlite_describe_nif/1, sqlite_interrupt_nif/1,
    sqlite_system_info_nif/0, sqlite_get_last_insert_rowid_nif/1, sqlite_finish_nif/1,
    sqlite_backup_init_nif/4, sqlite_backup_step_nif/2, sqlite_backup_finish_nif/1,
    sqlite_dirty_close_nif/0, sqlite_dirty_backup_finish_nif/0]).

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

-type parameter() :: integer() | float() | binary() | string() | iolist() | {binary, binary()} | undefined.
%% Erlang type allowed to be used in SQLite bindings.

-type parameters() :: [parameter()] | #{atom() | iolist() | pos_integer() => parameter()}.
%% Positional and named parameters.
%%
%% A list of parameters must match the expected number of parameters.
%% A map of parameters may be used to define named parameters, or
%% assign specific columns. When map key is an integer, it is treated
%% as a column index.

-type row() :: tuple().

-type prepare_options() :: #{
    persistent => boolean(),
    no_vtab => boolean()
}.
%% Prepared statement creation flags
%%
%% <ul>
%%   <li>`persistent': The SQLITE_PREPARE_PERSISTENT flag is a hint to the query planner that
%%      the prepared statement will be retained for a long time and probably reused many times</li>
%%   <li>`no_vtab': The SQLITE_PREPARE_NO_VTAB flag causes the SQL compiler to return an error
%%      (error code SQLITE_ERROR) if the statement uses any virtual tables</li>
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
    malloc => {Size :: integer(), Count :: integer(), Max :: integer()},
    version => binary()
}.
%% Status information about the performance of SQLite
%%
%% See `sqlite3_status' in the SQLite reference for more details about
%% exported statistics.

-type progress_callback() :: fun((Remaining :: non_neg_integer(), Total :: non_neg_integer()) ->
    ok | {ok, NewStep :: pos_integer()} | stop).
%% Backup progress callback.
%% Must return `ok' for backup to continue, `{ok, Steps}` to change the steps amount.
%% Return `stop' to stop the backup process. Throwing an exception also stops the process.

-type backup_options() :: #{
    from => unicode:chardata(),
    to => unicode:chardata(),
    step => pos_integer(),
    progress => progress_callback()
}.
%% Backup options
%%
%% <ul>
%%   <li>`from': source database name, `<<"main">>' is the default</li>
%%   <li>`to': destination database name, takes `from' value as the default</li>
%%   <li>`stop': backup step, pages. Default is 64</li>
%%   <li>`progress': progress callback to invoke after finishing the next step</li>
%% </ul>

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

%% @doc Closes the connection.
%%
%% Ensures that all prepared statements are deallocated. Forcibly
%% stops all running backups (both source and destination). Releases
%% the underlying database file.
%%
%% It is not necessary to call this function explicitly, as eventually
%% all resources will be collected. However it might be useful if you
%% need to unlock the database file immediately.
%%
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
-spec status(connection()) -> connection_status().
status(Connection) ->
    case sqlite_status_nif(Connection) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Connection], [{error_info, #{cause => ExtErr}}]);
        Status ->
            Status
    end.

%% @equiv query(Connection, Query, [])
-spec query(connection(), iodata()) -> [row()].
query(Connection, Query) ->
    query(Connection, Query, []).

%% @doc Runs an SQL query using specified connection
%%
%% Query may contain placeholders, e.g. `?', `?1', `?2'. Number of placeholders
%% must match the length of the parameters list. However for a map-based
%% parameters, there is no such limitation, and it is supported to bind a single
%% column using the following syntax: `#{2 => "value"}'.
%%
%% Named parameters are also supported:
%% ```
%% sqlite:query(Conn, "SELECT * FROM table WHERE key = :key", #{key => 1}).
%% '''
%% Missing named parameters are treated as NULL.
%%
%% Parameters are converted to sqlite storage classed using following type mappings:
%% <ul>
%%   <li>`binary()': accepts an Erlang binary stored as a TEXT in sqlite. See `unicode' module for
%%    transformations between Unicode characters list and a binary</li>
%%   <li>`string()': accepts an Erlang string limited to `iolist()' type, stored as a TEXT</li>
%%   <li>`iolist()': accepts lists supported by {@link erlang:iolist_to_binary/1}, stored as a TEXT</li>
%%   <li>`integer()': accepts a 64-bit signed integer (BIGNUM is not supported), stored as an INTEGER</li>
%%   <li>`float()': accepts IEEE floating point number, stored as a FLOAT</li>
%%   <li>`undefined': stored as NULL</li>
%%   <li>`{blob, binary()}': accepts an Erlang binary stored as a BLOB</li>
%% </ul>
%%
%% Returns a list of database rows, or an empty list if no rows are expected
%% to be returned (e.g. `CREATE TABLE' statement). By default, all TEXT fields are
%% returned as Erlang `binary()' type for performance reasons.
%%
%% BLOB is always returned as a binary. Note that wrapping in `{binary, Bin}' tuple is
%% only necessary for parameters, to tell the actual binary from an UTF8 encoded string.
-spec query(connection(), iodata(), parameters()) -> [row()].
query(Connection, Query, Parameter) ->
    case sqlite_query_nif(Connection, Query, Parameter) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Connection, Query, Parameter], [{error_info, #{cause => ExtErr}}]);
        Result when is_list(Result) ->
            lists:reverse(Result)
    end.

%% @equiv prepare(Connection, Query, #{})
-spec prepare(connection(), iodata()) -> prepared_statement().
prepare(Connection, Query) ->
    prepare(Connection, Query, #{}).

%% @doc Creates a prepared statement.
%%
%% Use {@link execute/2} to run the statement.
%% Note that {@link close/1} invalidates all prepared statements created
%% using the specified connection.
%%
%% By default, the prepared statement is not persistent. Note that
%% if a single statement is going to be reused many times, it may
%% prove useful to pass `persistent' option set to `true'.
%%
%% Running a single {@link query/3} once is more efficient than preparing
%% a statement, executing it once and discarding.
-spec prepare(connection(), iodata(), prepare_options()) -> prepared_statement().
prepare(Connection, Query, Options) ->
    case sqlite_prepare_nif(Connection, Query, Options) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Connection, Query, Options], [{error_info, #{cause => ExtErr}}]);
        Prepared ->
            Prepared
    end.

%% @doc Binds arguments to a prepared statement.
%%
%% Does not reset the statement, and throws `badarg' with SQL_MISUSE
%% explanation if the statement needs to be reset first.
%%
%% See {@link query/3} for types and bindings details.
-spec bind(prepared_statement(), parameters()) -> ok.
bind(Prepared, Parameters) ->
    case sqlite_bind_nif(Prepared, Parameters) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Prepared, Parameters], [{error_info, #{cause => ExtErr}}]);
        ok ->
            ok
    end.

%% @doc Evaluates the prepared statement.
%%
%% Returns `done' when an operation has completed.
%% May return `busy', see `SQLITE_BUSY' return code in
%% the sqlite reference manual.
-spec step(prepared_statement()) -> busy | done | row().
step(Prepared) ->
    case sqlite_step_nif(Prepared, 1) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Prepared], [{error_info, #{cause => ExtErr}}]);
        Result ->
            Result
    end.

%% @doc Evaluates the prepared statement.
%%
%% Returns either a list of rows when there are more rows available,
%% or `{done, Rows}` tuple with the list of the remaining rows.
%% May return `{busy, Rows}', requesting the caller to handle
%% `SQL_BUSY' according to sqlite recommendations.
-spec step(prepared_statement(), pos_integer()) -> [row()] | {done, [row()]} | {busy, [row()]}.
step(Prepared, Steps) when Steps > 1 ->
    case sqlite_step_nif(Prepared, Steps) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Prepared, Steps], [{error_info, #{cause => ExtErr}}]);
        {Code, Result} ->
            {Code, lists:reverse(Result)};
        List ->
            lists:reverse(List)
    end.

%% @doc Resets the prepared statement, but does not clear bindings
-spec reset(prepared_statement()) -> ok.
reset(Prepared) ->
    case sqlite_reset_nif(Prepared) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Prepared], [{error_info, #{cause => ExtErr}}]);
        Result ->
            Result
    end.

%% @doc Clears bindings of the prepared statement
-spec clear(prepared_statement()) -> ok.
clear(Prepared) ->
    case sqlite_clear_nif(Prepared) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Prepared], [{error_info, #{cause => ExtErr}}]);
        Result ->
            Result
    end.

%% @doc Finalises the prepared statement, freeing any resources allocated
%%
%% The purpose of this function is to cancel statements that are executed
%% halfway through, and it is desirable to stop execution and free all
%% allocated resources.
-spec finish(prepared_statement()) -> ok.
finish(Prepared) ->
    case sqlite_finish_nif(Prepared) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Prepared], [{error_info, #{cause => ExtErr}}]);
        Result ->
            Result
    end.

%% @doc Returns column names and types for the prepared statement.
%%
%% SQLite uses dynamic type system. Types returned are column types specified
%% in the `CREATE TABLE' statement. SQLite accepts any string as a type name,
%% unless `STRICT' mode is used.
-spec describe(prepared_statement()) -> [{Name :: binary(), Type :: binary()}].
describe(Prepared) ->
    case sqlite_describe_nif(Prepared) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Prepared], [{error_info, #{cause => ExtErr}}]);
        Description ->
            Description
    end.

%% @doc Returns runtime statistics for the prepared statement.
-spec info(prepared_statement()) -> statement_info().
info(Prepared) ->
    case sqlite_info_nif(Prepared) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Prepared], [{error_info, #{cause => ExtErr}}]);
        Description ->
            Description
    end.

%% @doc Runs the prepared statement with new parameters bound.
%%
%% Resets the prepared statement, binds new parameters and
%% steps until no more rows are available. Throws an error
%% if at any step `SQLITE_BUSY' was returned, see {@link sqlite:connect_options()}
%% for busy timeout handling.
%%
%% See {@link query/3} for types and bindings details.
-spec execute(prepared_statement(), parameters()) -> [row()].
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

%% @doc EXPERIMENTAL: Stops monitoring previously monitored connection.
%%
%% Does not flush messages that are already in transit.
%% This API is experimental and may change in the future.
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
%% `close/1' on the same connection.
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

%% @doc Backs up the main database of the source to the destination.
-spec backup(Source :: connection(), Destination :: connection()) -> ok.
backup(Source, Destination) ->
    backup(Source, Destination, #{from => <<"main">>, to => <<"main">>}).

%% @doc Runs a backup with the options specified
%%
%% Returns `ok' if the backup was successful, or `stop'
%% if the process was requested to stop.
-spec backup(Source :: connection(), Destination :: connection(), Options :: backup_options()) -> ok | stop.
backup(Source, Destination, Options) ->
    SrcDb = maps:get(from, Options, <<"main">>),
    DstDb = maps:get(to, Options, SrcDb),
    Step = maps:get(step, Options, 64),
    case sqlite_backup_init_nif(Source, SrcDb, Destination, DstDb) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Source, Destination, Options], [{error_info, #{cause => ExtErr}}]);
        Ref ->
            try
                do_backup(Ref, Step, maps:get(progress, Options, undefined))
            after
                sqlite_backup_finish_nif(Ref)
            end
    end.

%%-------------------------------------------------------------------
%% Internal implementation

do_backup(Ref, Step, Progress) ->
    case sqlite_backup_step_nif(Ref, Step) of
        {_Remaining, _Total} when Progress =:= undefined ->
            do_backup(Ref, Step, Progress);
        {Remaining, Total} ->
            case Progress(Remaining, Total) of
                ok ->
                    do_backup(Ref, Step, Progress);
                {ok, NewStep} ->
                    do_backup(Ref, NewStep, Progress);
                stop ->
                    stop
            end;
        ok ->
            ok;
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Ref, Step], [{error_info, #{cause => ExtErr}}])
    end.

%% @doc Formats exception according to EEP-54.
%%
%% Used internally by the shell exception handler.
%% Note that NIFs are not yet supported, and therefore exceptions are
%% thrown by Erlang code. Use `erl_error' module to provide human-readable
%% exception explanations.
format_error(_Reason, [{_M, _F, _As, Info} | _]) ->
    #{cause := Cause} = proplists:get_value(error_info, Info, #{}),
    Cause.


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

sqlite_bind_nif(_Prepared, _Parameters) ->
    ?nif_stub.

sqlite_step_nif(_Prepared, _Steps) ->
    ?nif_stub.

sqlite_reset_nif(_Prepared) ->
    ?nif_stub.

sqlite_clear_nif(_Prepared) ->
    ?nif_stub.

sqlite_finish_nif(_Prepared) ->
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

sqlite_backup_init_nif(_Src, _SrcDb, _Dst, _DstDb) ->
    ?nif_stub.

sqlite_backup_step_nif(_Backup, _Pages) ->
    ?nif_stub.

sqlite_backup_finish_nif(_Backup) ->
    ?nif_stub.

sqlite_dirty_close_nif() ->
    ?nif_stub.

sqlite_dirty_backup_finish_nif() ->
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
            delayed_dealloc();
        general ->
            sqlite_dirty_backup_finish_nif(),
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
