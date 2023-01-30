%%%-------------------------------------------------------------------
%%% @copyright (C) 2023 Maxim Fedorov
%%% Tests for SQLite Connection API
-module(sqlite_SUITE).
-author("maximfca@gmail.com").

%% Common Test API
-export([suite/0, all/0]).

%% Test cases
-export([basic/1, crash/1, types/1, close/1, status/1, race_close_prepare/1,
    allocators/1, prepared/1, errors/1, monitor/1, concurrency/1]).

-export([benchmark_prepared/1]).

-behaviour(ct_suite).

-include_lib("stdlib/include/assert.hrl").

suite() ->
    [].
    %%[{timetrap, {seconds, 10}}].

all() ->
    [basic, crash, types, close, status, allocators, prepared, errors, monitor, concurrency,
        race_close_prepare].

%%-------------------------------------------------------------------
%% Convenience functions
-define(QUERY, "SELECT * FROM kv WHERE key=?1").

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

%%-------------------------------------------------------------------
%% TEST CASES

basic(Config) when is_list(Config) ->
    Conn = sqlite:open("", #{mode => in_memory}),
    ok = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val TEXT) STRICT"),
    %% create a table with key -> value columns
    %% insert 1 => str
    ok = sqlite:query(Conn, "INSERT INTO kv (key, val) VALUES (1, 'str')"),
    Unicode = unicode:characters_to_binary("юникод"),
    ok = sqlite:query(Conn, "INSERT INTO kv (key, val) VALUES (?1, ?2)", [2, Unicode]),
    ?assertEqual(2, sqlite:get_last_insert_rowid(Conn)),
    %% select
    [{1, <<"str">>}, {2, Unicode}] = sqlite:query(Conn, "SELECT * from kv ORDER BY key").

types(Config) when is_list(Config) ->
    Conn = sqlite:open("", #{mode => in_memory}),
    ok = sqlite:query(Conn, "CREATE TABLE types (t1 TEXT, b1 BLOB, i1 INTEGER, f1 REAL) STRICT"),
    %% binary
    %% string
    %% integer. Test small and bignum
    PreparedInt = sqlite:prepare(Conn, "INSERT INTO types (i1) VALUES ($1)"),
    ok = sqlite:execute(PreparedInt, [576460752303423489]), %% bigint but fits
    ok = sqlite:execute(PreparedInt, [-576460752303423490]), %% bigint but fits
    ok = sqlite:execute(PreparedInt, [-576460752303423489]), %% small int
    ok = sqlite:execute(PreparedInt, [576460752303423488]), %% small int
    [{-576460752303423490}, {-576460752303423489}, {576460752303423488}, {576460752303423489}] =
        sqlite:query(Conn, "SELECT i1 FROM types WHERE i1 IS NOT NULL ORDER BY i1"),
    ?assertExtended(error, badarg,
        #{cause => #{position => 1, general => <<"failed to bind argument">>, details => <<"bignum not supported">>}},
        sqlite:execute(PreparedInt, [18446744073709551615])),
    %% float
    PreparedFloat = sqlite:prepare(Conn, "INSERT INTO types (f1) VALUES ($1)"),
    Pi = math:pi(),
    NegativePi = -Pi,
    ok = sqlite:execute(PreparedFloat, [Pi]),
    ok = sqlite:execute(PreparedFloat, [NegativePi]),
    [{NegativePi}, {Pi}] = sqlite:query(Conn, "SELECT f1 FROM types WHERE f1 IS NOT NULL ORDER BY f1"),
    %% null (undefined)
    %% atom (??!)
    %% reference/fun/pid/port/THING
    ?assertExtended(error, badarg,
        #{cause => #{details => <<"unsupported type">>, position => 1, general => <<"failed to bind argument">>}},
        sqlite:execute(PreparedInt, [self()])),
    %% tuple/record
    %% map (JSON?)
    %% list
    ok.

status(Config) when is_list(Config) ->
    Conn = sqlite:open("", #{mode => in_memory}),
    ok = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val TEXT) STRICT"),
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
    Conn = sqlite:open("", #{mode => in_memory}),
    ok = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val TEXT) STRICT"),
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
    Conn = sqlite:open("", #{mode => in_memory}),
    ok = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val TEXT) STRICT"),
    PreparedInsert = sqlite:prepare(Conn, "INSERT INTO kv (key, val) VALUES (?1, ?2)"),
    ok = sqlite:execute(PreparedInsert, [1, "string"]),
    PreparedSel = sqlite:prepare(Conn, "SELECT * FROM kv WHERE key = ?1"),
    [{1, <<"string">>}] = sqlite:execute(PreparedSel, [1]),
    %% test statement into
    ?assertEqual([{<<"key">>,<<"INTEGER">>},{<<"val">>,<<"TEXT">>}], sqlite:describe(PreparedSel)).

monitor(Config) when is_list(Config) ->
    Conn = sqlite:open("", #{mode => in_memory}),
    ok = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val TEXT) STRICT"),
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

close(Config) when is_list(Config) ->
    Conn = sqlite:open("", #{mode => in_memory}),
    ok = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val TEXT) STRICT"),
    ok = sqlite:close(Conn),
    ?assertExtended(error, badarg, #{cause => #{1 => <<"connection closed">>}},
        sqlite:prepare(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val TEXT) STRICT")),
    ?assertExtended(error, badarg, #{cause => #{1 => <<"connection already closed">>}}, sqlite:close(Conn)).

crash(Config) when is_list(Config) ->
    %% ensures that even after GC prepared statement is working
    Conn = sqlite:open("", #{mode => in_memory}),
    Prepared = sqlite:prepare(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val TEXT) STRICT"),
    sqlite:close(Conn),
    erlang:garbage_collect(),
    attempt_crash(Prepared).

attempt_crash(Prepared) ->
    ?assertExtended(error, badarg, #{cause => #{1 => <<"connection closed">>}},
        sqlite:execute(Prepared, [<<"1">>])).

errors(Config) when is_list(Config) ->
    %% test EPP-54 extended error information
    ?assertExtended(error, badarg, #{cause => #{2 => <<"not a map">>}}, sqlite:open("", 123)),
    ?assertExtended(error, badarg, #{cause => #{2 => <<"unsupported mode">>}}, sqlite:open("", #{mode => invalid})),
    ?assertExtended(error, {sqlite_error, 14}, #{cause => #{general => <<"error opening connection">>,
        details => <<"unable to open database file">>}},
        sqlite:open("not_exist", #{mode => read_only})),
    %% need a statement for testing
    Db = sqlite:open("", #{mode => in_memory}),
    ok = sqlite:query(Db, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val STRING)"),
    %% errors while preparing query
    ?assertExtended(error, {sqlite_error, 1}, #{cause => #{general => <<"error preparing statement">>,
        details => <<"SQL logic error">>, position => 30}},
        sqlite:query(Db, "SELECT * from kv ORDER BY key WHERE key = ?", [1])),
    %% extra binding when not expected
    ?assertExtended(error, {sqlite_error,25},
        #{cause => #{position => 2, general => <<"failed to bind argument">>, details => <<"column index out of range">>}},
        sqlite:query(Db, "INSERT INTO kv (key) VALUES ($1)", [1, "1", 1])),
    %% not enough bindings
    ?assertExtended(error, badarg, #{cause => #{2 => <<"not enough arguments">>}},
        sqlite:query(Db, "INSERT INTO kv (key, val) VALUES ($1, $2)", [1])),
    sqlite:close(Db).

concurrency(Config) when is_list(Config) ->
    Concurrency = erlang:system_info(schedulers_online) * 8,
    FileName = filename:join(proplists:get_value(priv_dir, Config), "db.bin"),
    %% create a DB with schema
    Db = sqlite:open(FileName),
    ok = sqlite:query(Db, "CREATE TABLE kv (key INTEGER PRIMARY KEY AUTOINCREMENT, val INTEGER)"),
    sqlite:close(Db),
    %% share a connection (and prepared statements) between many workers using ETS
    Workers = [erlang:spawn_link(fun() -> worker(Seq, FileName) end) || Seq <- lists:seq(1, Concurrency)],
    Monitors = [{W, erlang:monitor(process, W)} || W <- Workers],
    %% wait for all of them to complete (with no error, otherwise the link crashes test runner)
    [receive {'DOWN', Mon, process, Pid, Reason} -> Reason end || {Pid, Mon} <- Monitors].

worker(Seq, FileName) ->
    worker(500, Seq, sqlite:open(FileName, #{busy_timeout => 60000}), FileName, []).

%% Possible actions:
%%  * open/close a DB
%%  * send DB to another process/receive DB
%%  * create a statement/destroy
%%  * execute a statement
worker(0, _Seed, _Db, _FileName, _Statements) ->
    ok;
worker(Count, Seed, Db, FileName, Statements) ->
    Next = rand:mwc59(Seed),
    case Next rem 80 of
        0 ->
            ok = sqlite:close(Db),
            NewDb = sqlite:open(FileName, #{busy_timeout => 60000}),
            worker(Count - 1, Next, NewDb, FileName, []);
        NewStatement when NewStatement < 10 ->
            try
                Prep = sqlite:prepare(Db, "INSERT INTO kv (val) VALUES (?1)"),
                worker(Count - 1, Next, Db, FileName, [Prep | Statements])
            catch
                TX:TY ->
                    io:format(user, "prepare: ~s:~p~n", [TX, TY]),
                    worker(Count, Next, Db, FileName, Statements)
            end;
        DelStatement when DelStatement < 20, length(Statements) > 0 ->
            NewStmts = lists:droplast(Statements),
            worker(Count - 1, Next, Db, FileName, NewStmts);
        _RunStatement when length(Statements) > 0 ->
            try sqlite:execute(hd(Statements), [1])
            catch X:Y -> io:format(user, "exec: ~s:~p~n", [X, Y])
            end,
            worker(Count - 1, Next, Db, FileName, Statements);
        _RunStatement ->
            %% skipping a step
            worker(Count, Next, Db, FileName, Statements)
    end.

race_close_prepare(Config) when is_list(Config) ->
    do_race(10000).

do_race(0) -> ok;
do_race(Count) ->
    Conn = sqlite:open("", #{mode => in_memory}),
    ok = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY AUTOINCREMENT, val INTEGER)"),
    %%
    Preparer = spawn_link(fun() -> do_prepare(1000, Conn) end),
    Closer = spawn_link(fun() -> timer:sleep(2), sqlite:close(Conn) end),
    %%
    Workers = [Preparer, Closer],
    Monitors = [{W, erlang:monitor(process, W)} || W <- Workers],
    %% wait for all of them to complete (with no error, otherwise the link crashes test runner)
    [receive {'DOWN', Mon, process, Pid, Reason} -> Reason end || {Pid, Mon} <- Monitors],
    do_race(Count - 1).

do_prepare(0, _Conn) ->
    ?assert(too_quick);
do_prepare(Count, Conn) ->
    try sqlite:prepare(Conn, ?QUERY), do_prepare(Count - 1, Conn)
    catch error:badarg -> ok end.

%% benchmark for prepared statements
benchmark_prepared(Config) when is_list(Config) ->
    measure_one(fun bench_query/2, "query"),
    measure_one(fun bench_prep/2, "prepare every time"),
    measure_one(fun bench_one/2, "prepare once").

measure_one(FunTo, Kind) ->
    Conn = sqlite:open("", #{mode => in_memory}),
    ok = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY AUTOINCREMENT, val INTEGER)"),
    [erlang:garbage_collect(Pid) || Pid <- processes()], %% ensure no garbage in the system
    MemBefore = erlang:memory(system),
    {TimeUs, _} = timer:tc(fun () -> FunTo(100000, Conn), erlang:garbage_collect() end),
    MemDiff = erlang:memory(system) - MemBefore,
    sqlite:close(Conn),
    erlang:garbage_collect(),
    MemZero = erlang:memory(system) - MemBefore,
    ct:pal("~s: ~b ms, allocated ~b Kb (~b Kb)", [Kind, TimeUs div 1000, MemDiff div 1024, MemZero div 1024]),
    {TimeUs, MemDiff}.

bench_query(0, _Conn) ->
    ok;
bench_query(Count, Conn) ->
    sqlite:query(Conn, ?QUERY, [1]),
    bench_query(Count - 1, Conn).

bench_prep(0, _Conn) ->
    ok;
bench_prep(Count, Conn) ->
    Prep = sqlite:prepare(Conn, ?QUERY),
    sqlite:execute(Prep, [1]),
    bench_prep(Count - 1, Conn).

bench_one(Count, Conn) ->
    do_bench_one(Count, sqlite:prepare(Conn, ?QUERY)).

do_bench_one(0, _Prep) ->
   ok;
do_bench_one(Count, Prep) ->
    sqlite:execute(Prep, [1]),
    do_bench_one(Count - 1, Prep).
