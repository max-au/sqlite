%%%-------------------------------------------------------------------
%%% @copyright (C) 2023 Maxim Fedorov
%%% Tests for SQLite Connection API
-module(sqlite_SUITE).
-author("maximfca@gmail.com").

%% Common Test API
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([basic/1, close/1, status/1, allocators/1, prepared/1, errors/1, monitor/1]).

-behaviour(ct_suite).

-include_lib("stdlib/include/assert.hrl").

all() ->
    [basic, close, status, allocators, prepared, errors, monitor].

init_per_testcase(NoDb, Config) when NoDb =:= close; NoDb =:= errors ->
    Config;
init_per_testcase(_TestCase, Config) ->
    Conn = sqlite:open("", #{mode => in_memory}),
    ok = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val TEXT) STRICT"),
    [{db, Conn} | Config].

end_per_testcase(_TestCase, Config) ->
    case proplists:get_value(db, Config, undefined) of
        undefined ->
            Config;
        Conn ->
            sqlite:close(Conn),
            proplists:delete(db, Config)
    end.

%%-------------------------------------------------------------------
%% TEST CASES

basic(Config) when is_list(Config) ->
    Conn = proplists:get_value(db, Config),
    %% create a table with key -> value columns
    %% insert 1 => str
    ok = sqlite:query(Conn, "INSERT INTO kv (key, val) VALUES (1, 'str')"),
    Unicode = unicode:characters_to_binary("юникод"),
    ok = sqlite:query(Conn, "INSERT INTO kv (key, val) VALUES (?1, ?2)", [2, Unicode]),
    ?assertEqual(2, sqlite:get_last_insert_rowid(Conn)),
    %% select
    [{1, <<"str">>}, {2, Unicode}] = sqlite:query(Conn, "SELECT * from kv ORDER BY key").

status(Config) when is_list(Config) ->
    Conn = proplists:get_value(db, Config),
    Prepared = sqlite:prepare(Conn, "INSERT INTO kv (key, val) VALUES (1, ?1)"),
    %% sanity check, that fields are present, but not beyond that. Should are real assertions here
    #{cache := Cache, deferred_fks := 0, statement := _,  lookaside := Lookaside} = sqlite:status(Conn),
    #{used := Used, shared := _Shared, hit := Hit, miss := _Miss, write := _Write, spill := _Spill} = Cache,
    #{used := _, max := _Max, hit := _, miss_size := _, miss_full := _} = Lookaside,
    ?assert(Used > 0, {used, Used}),
    ?assert(Hit > 0, {hit, Hit}),
    %% prepared statement stats
    #{fullscan_step := 0, sort := 0, autoindex := 0, vm_step := 0, reprepare := 0,
        run := 0, memory_used := _} = sqlite:info(Prepared),
    %%  system stats, skip page_cache check for not all versions have it
    #{memory_used := {_, _}, page_cache := {_, _, _, _, _}, malloc := {_, _, _}} = sqlite:system_info().

allocators(Config) when is_list(Config) ->
    Conn = proplists:get_value(db, Config),
    Bin = iolist_to_binary([<<"1234567890ABCDEF">> || _ <- lists:seq(1, 128)]),
    Repeats = 16,
    BinSize = byte_size(Bin) * Repeats,
    Statement = sqlite:prepare(Conn, "INSERT INTO kv (key, val) VALUES (?1, ?2)"),
    %% test that erlang:memory() reflects changes when large allocations are done
    Before = erlang:memory(system),
    [ok = sqlite:execute(Statement, [Seq, Bin]) || Seq <- lists:seq(1, Repeats)],
    Diff = erlang:memory(system) - Before,
    %% system memory should grow ~2x of the binding size
    ?assert(Diff > BinSize andalso Diff < (3 * BinSize), {difference, Diff, byte_size, BinSize}).

prepared(Config) when is_list(Config) ->
    Conn = proplists:get_value(db, Config),
    Statement = sqlite:prepare(Conn, "INSERT INTO kv (key, val) VALUES (?1, ?2)"),
    ok = sqlite:execute(Statement, [1, "string"]),
    SelStmt = sqlite:prepare(Conn, "SELECT * FROM kv WHERE key = ?1"),
    [{1, <<"string">>}] = sqlite:execute(SelStmt, [1]),
    %% test statement into
    ?assertEqual([{<<"key">>,<<"INTEGER">>},{<<"val">>,<<"TEXT">>}], sqlite:describe(SelStmt)).

monitor(Config) when is_list(Config) ->
    Conn = proplists:get_value(db, Config),
    Ref = sqlite:monitor(Conn),
    %% verify monitoring works
    ok = sqlite:query(Conn, "INSERT INTO kv (key, val) VALUES (1, 'str')"),
    receive
        {Ref, insert, <<"main">>, <<"kv">>, _RowId} -> ok
    after 10 -> ?assert(false, "monitoring did not work, expected message is not received")
    end,
    %% test demonitoring
    sqlite:demonitor(Ref),
    ok = sqlite:query(Conn, "INSERT INTO kv (key, val) VALUES (2, 'str')"),
    receive Unexpected2 -> ?assert(false, {unexpected, Unexpected2})
    after 10 -> ok
    end.

-define(assertExtended(Class, Term, ErrorInfo, Expr),
    begin
        ((fun () ->
            try (Expr) of
                X__V ->
                    erlang:error({assertExtended, [{module, ?MODULE}, {line, ?LINE},
                        {expression, (??Expr)}, {pattern, "{ "++(??Class)++" , "++ (??Term) ++" , [...] }"},
                        {unexpected_success, X__V}]})
            catch
                Class:Term:X__S ->
                    {_, _, _, X__E} = hd(X__S),
                    case lists:keyfind(error_info, 1, X__E) of
                        {error_info, X__EWI} when X__EWI =:= ErrorInfo ->
                            ok;
                        {error_info, X__EWI} ->
                            erlang:error({assertExtended, [{module, ?MODULE}, {line, ?LINE},
                                {expression, (??Expr)}, {pattern, "{ "++(??Class)++" , "++(??Term) ++" , [...] }"},
                                {unexpected_extended_error, {Class, Term, X__EWI}}]});
                        false ->
                            erlang:error({assertExtended, [{module, ?MODULE}, {line, ?LINE},
                                {expression, (??Expr)}, {pattern, "{ "++(??Class)++" , "++(??Term) ++" , [...] }"},
                                {missing_extended_error, {Class, Term, X__E}}]})
                    end;
                X__C:X__T:X__S ->
                    erlang:error({assertExtended, [{module, ?MODULE}, {line, ?LINE},
                        {expression, (??Expr)}, {pattern, "{ "++(??Class)++" , "++(??Term) ++" , [...] }"},
                        {unexpected_exception, {X__C, X__T, X__S}}]})
            end
          end)())
    end).

close(Config) when is_list(Config) ->
    Conn = sqlite:open("", #{mode => in_memory}),
    ok = sqlite:close(Conn),
    ?assertExtended(error, badarg, #{cause => #{1 => <<"connection already closed">>}}, sqlite:close(Conn)).

errors(Config) when is_list(Config) ->
    %% test EPP-54 extended error information
    ?assertExtended(error, badarg, #{cause => #{2 => <<"not a map">>}}, sqlite:open("", 123)),
    ?assertExtended(error, badarg, #{cause => #{2 => <<"unsupported mode">>}}, sqlite:open("", #{mode => invalid})),
    ?assertExtended(error, {sqlite_error, 14}, #{cause => #{extra => <<"error opening connection">>,
        general => <<"unable to open database file">>}},
        sqlite:open("not_exist", #{mode => read_only})),
    %% need a statement for testing
    Db = sqlite:open("", #{mode => in_memory}),
    ok = sqlite:query(Db, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val STRING)"),
    %% errors while preparing query
    ?assertExtended(error, {sqlite_error, 1}, #{cause => #{extra => <<"error preparing statement">>,
        general => <<"SQL logic error">>}},
        sqlite:query(Db, "SELECT * from kv ORDER BY key WHERE key = ?", [1])),
    sqlite:close(Db).

%% need a full "property-like" test to generate all types and corner cases (BIGINT, negative, positive, signed, unsigned)