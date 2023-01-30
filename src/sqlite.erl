%%%-------------------------------------------------------------------
%%% @copyright (C) 2023 Maxim Fedorov
%%% sqlite3 NIF bindings
%%% TODO: implement message-based asynchronous APIs
-module(sqlite).
-author("maximfca@gmail.com").

%% API
-export([open/1, open/2, close/1, status/1,
    query/2, query/3, prepare/2, info/1, describe/1, execute/2,
    monitor/1, demonitor/1, interrupt/1, system_info/0, get_last_insert_rowid/1]).
-export([format_error/2, delayed_dealloc/0]).
-on_load(init/0).
-nifs([sqlite_open_nif/2, sqlite_close_nif/1, sqlite_status_nif/1,
    sqlite_query_nif/3, sqlite_prepare_nif/2, sqlite_info_nif/1, sqlite_execute_nif/2,
    sqlite_monitor_nif/2, sqlite_describe_nif/1, sqlite_interrupt_nif/1,
    sqlite_system_info_nif/0, sqlite_get_last_insert_rowid_nif/1, sqlite_dirty_close_nif/0]).

-define(nif_stub, nif_stub_error(?LINE)).
nif_stub_error(Line) ->
    erlang:nif_error({nif_not_loaded,module, ?MODULE, line, Line}).

-type connection() :: reference().

-type connect_options() :: #{
    mode => read_only | read_write | read_write_create | in_memory,
    uri => boolean(),
    busy_timeout => non_neg_integer()
}.

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
-type binding() :: integer() | float() | binary() | string() | iolist().

%% Only a subset of Erlang types are allowed in returned rows.
%% However Erlang spec does not allow to express that.
-type row() :: tuple().

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

%% @doc Open the specified sqlite database.
-spec open(file:filename_all()) -> connection().
open(FileName) ->
    open(FileName, #{}).

%% @doc Open the specified sqlite database.
-spec open(file:filename_all(), Options :: connect_options()) -> connection().
open(FileName, Options) ->
    case sqlite_open_nif(FileName, Options) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [FileName, Options], [{error_info, #{cause => ExtErr}}]);
        Connection ->
            Connection
    end.

%% @doc Close the connection.
-spec close(connection()) -> ok.
close(Connection) ->
    case sqlite_close_nif(Connection) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Connection], [{error_info, #{cause => ExtErr}}]);
        ok ->
            ok
    end.

-spec status(connection()) -> connection_status().
status(Connection) ->
    case sqlite_status_nif(Connection) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Connection], [{error_info, #{cause => ExtErr}}]);
        Status ->
            Status
    end.

%% @doc Runs an SQL query using specified connection
-spec query(connection(), iodata()) -> ok | [row()].
query(Connection, Query) ->
    query(Connection, Query, []).

-spec query(connection(), iodata(), [binding()]) -> ok | [row()].
query(Connection, Query, Bindings) ->
    case sqlite_query_nif(Connection, Query, Bindings) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Connection, Query, Bindings], [{error_info, #{cause => ExtErr}}]);
        ok ->
            ok;
        Result when is_list(Result) ->
            lists:reverse(Result)
    end.

-spec prepare(connection(), iodata()) -> prepared_statement().
prepare(Connection, Query) ->
    case sqlite_prepare_nif(Connection, Query) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Connection, Query], [{error_info, #{cause => ExtErr}}]);
        Prepared ->
            Prepared
    end.

-spec describe(prepared_statement()) -> [{column_name(), column_type()}].
describe(Prepared) ->
    case sqlite_describe_nif(Prepared) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Prepared], [{error_info, #{cause => ExtErr}}]);
        Description ->
            Description
    end.

-spec info(prepared_statement()) -> statement_info().
info(Prepared) ->
    case sqlite_info_nif(Prepared) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Prepared], [{error_info, #{cause => ExtErr}}]);
        Description ->
            Description
    end.

-spec execute(prepared_statement(), [binding()]) -> ok | [row()].
execute(Prepared, Bindings) ->
    case sqlite_execute_nif(Prepared, Bindings) of
        {error, Reason, ExtErr} ->
            erlang:error(Reason, [Prepared, Bindings], [{error_info, #{cause => ExtErr}}]);
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

-spec interrupt(connection()) -> ok.
interrupt(Connection) ->
    sqlite_interrupt_nif(Connection).

-spec system_info() -> system_info().
system_info() ->
    sqlite_system_info_nif().

-spec get_last_insert_rowid(connection()) -> integer().
get_last_insert_rowid(Connection) ->
    sqlite_get_last_insert_rowid_nif(Connection).

%% @doc Formats exception according to EEP-54. Note that NIFs
%%      are not yet supported, and therefore exceptions are
%%      thrown by Erlang code.
format_error(Reason, [{_M, _F, _As, Info} | _]) ->
    #{cause := Cause} = proplists:get_value(error_info, Info, #{}),
    Cause#{reason => lists:flatten(io_lib:format("~p", [Reason]))}.

%%-------------------------------------------------------------------
%% NIF stubs

sqlite_open_nif(_FileName, _Options) ->
    ?nif_stub.

sqlite_close_nif(_Connection) ->
    ?nif_stub.

sqlite_status_nif(_Connection) ->
    ?nif_stub.

sqlite_query_nif(_Connection, _Query, _Bindings) ->
    ?nif_stub.

sqlite_prepare_nif(_Connection, _Query) ->
    ?nif_stub.

sqlite_info_nif(_Prepared) ->
    ?nif_stub.

sqlite_describe_nif(_Prepared) ->
    ?nif_stub.

sqlite_execute_nif(_Prepared, _Bindings) ->
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
    Pid = spawn(?MODULE, delayed_dealloc, []),
    Priv = code:priv_dir(sqlite),
    SoFile = filename:join(Priv, ?MODULE_STRING),
    ok = erlang:load_nif(SoFile, Pid).