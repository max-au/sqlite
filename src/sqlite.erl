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

%% Internal export for erl_error formatter
-export([format_error/2]).

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

-type lookaside() :: #{
    used => integer(),
    max => integer(),
    hit => integer(),
    miss_size => integer(),
    miss_full => integer()
}.

-type cache() :: #{
    used => integer(),
    shared => integer(),
    hit => integer(),
    miss => integer(),
    write => integer(),
    spill => {Current :: integer(), Max :: integer()}
}.

-type connection_status() :: #{
    lookaside => lookaside(),
    cache => cache(),
    schema => integer(),
    statement => integer(),
    deferred_fks => integer()
}.

%% Prepared statement
-type prepared_statement() :: reference().

%% Accepted types
-type column_type() :: integer | float | binary.

%% Column name
-type column_name() :: binary().

%% Only a subset of Erlang types is allowed in sqlite
-type parameter() :: integer() | float() | binary() | string() | iolist().

%% Only a subset of Erlang types are allowed in returned rows.
%% However Erlang spec does not allow to express that.
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

%% Prepared statement statistics
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

-type system_info() :: #{
    memory_used => {Cur :: integer(), Max :: integer()},
    page_cache => {Size :: integer(), Used :: integer(), Max :: integer(), Overflow :: integer(), OverflowMax :: integer()},
    malloc => {Size :: integer(), Count :: integer(), Max :: integer()}
}.

%% Extended error information thrown by NIFs
-type cause() :: #{
    general => unicode:chardata(),  %% sub-operation that failed "error opening connection"
    reason => unicode:chardata(),   %% formatted user-readable error
    pos_integer() => unicode:chardata(), %% argument that caused a problem, and a problem explanation
    position => integer()           %% only set for preparing statements or binding arguments (column that had an error)
}.

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
-spec query(connection(), iodata()) -> ok | [row()].
query(Connection, Query) ->
    query(Connection, Query, []).

%% @doc Runs an SQL query using specified connection
%%
%% Returns `ok' for operations that do not assume any output, or a list of
%% database rows.
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
%% Returned database rows follow the same conversion rules.
%%
%% Throws `badarg' if connection is closed.
-spec query(connection(), iodata(), [parameter()]) -> ok | [row()].
query(Connection, Query, Parameter) ->
    case sqlite_query_nif(Connection, Query, Parameter) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Connection, Query, Parameter], [{error_info, #{cause => ExtErr}}]);
        ok ->
            ok;
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

%% @doc Runs the prepared statement with new parameters passed.
%%
%% See `query/3' for types and bindings details.
%% Throws `badarg' if the connection is closed.
-spec execute(prepared_statement(), [parameter()]) -> ok | [row()].
execute(Prepared, Parameters) ->
    case sqlite_execute_nif(Prepared, Parameters) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Prepared, Parameters], [{error_info, #{cause => ExtErr}}]);
        ok ->
            ok;
        Result when is_list(Result) ->
            lists:reverse(Result)
    end.

-spec monitor(connection()) -> reference().
monitor(Connection) ->
    case sqlite_monitor_nif(Connection, self()) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Connection], [{error_info, #{cause => ExtErr}}]);
        Ref ->
            Ref
    end.

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
%% Throws `badarg' if the connection is closed.
-spec interrupt(connection()) -> ok.
interrupt(Connection) ->
    sqlite_interrupt_nif(Connection).

%% @doc Returns SQLite system information (memory usage, page cache usage, allocation statistics)
-spec system_info() -> system_info().
system_info() ->
    sqlite_system_info_nif().

-spec get_last_insert_rowid(connection()) -> integer().
get_last_insert_rowid(Connection) ->
    sqlite_get_last_insert_rowid_nif(Connection).

%% @doc Formats exception according to EEP-54.
%%
%% Used internally by the shell exception handler.
%% Note that NIFs are not yet supported, and therefore exceptions are
%% thrown by Erlang code. Use `erl_error' module to provide human-readable
%% exception explanations.
%%
%%
-spec format_error(term(), list()) -> cause().
%format_error({sqlite_error, Num}, [{_M, _F, Args, Info} | _]) ->
%    erlang:display({sqlite_handler, '**************************', Num, Info}),
%    #{cause := Cause} = proplists:get_value(error_info, Info, #{}),
    %#{cause := #{details := Details} = Cause} = proplists:get_value(error_info, Info, #{}),
    %Reason =
%        case maps:find(position, Cause) of
%            {ok, Col} ->
%                [_, Arg | _] = Args,
%                [From | _] = string:split(string:slice(Arg, Col), " "),
%                lists:flatten(io_lib:format("SQLite Error ~b: ~s: at: ~b: near \"~s\"", [Num, Details, Col, From]));
%            error ->
%                %lists:flatten(io_lib:format("SQLite Error ~b: ~s", [Num, Details]))
%        end,
%    Cause#{reason => Reason};
%    Cause;
%format_error(badarg, [{_M, _F, _As, Info} | _]) ->
%    erlang:display({badarg_handler, '**************************', Info}),
%    #{cause := Cause} = proplists:get_value(error_info, Info, #{}),
%    Cause#{reason => lists:flatten(io_lib:format("~p", [badarg]))};
format_error(Other, [{_M, _F, _As, Info} | _]) ->
%    erlang:display({other_handler, '**************************', Other}),
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
