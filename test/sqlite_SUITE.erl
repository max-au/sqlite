%%%-------------------------------------------------------------------
%%% @copyright (C) 2023 Maxim Fedorov
%%% Tests for SQLite Connection API
-module(sqlite_SUITE).
-author("maximfca@gmail.com").

%% Common Test API
-export([suite/0, all/0, init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([basic/1,
    bind_step/0, bind_step/1,
    shared/0, shared/1,
    shared_cache/0, shared_cache/1,
    interrupt/0, interrupt/1,
    crash/1, types/1, close/1, status/1,
    race_close_prepare/0, race_close_prepare/1,
    enif_alloc/0, enif_alloc/1,
    malloc/0, malloc/1,
    backup/1, backup_sanity/0, backup_sanity/1,
    named_parameters/0, named_parameters/1,
    prepared/1, errors/1, monitor/1,
    concurrency/0, concurrency/1,
    delayed_dealloc_kill/1]).

-behaviour(ct_suite).

-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 10000}}].

init_per_testcase(TC, Config) when TC =:= malloc ->
    ensure_unload(sqlite),
    Config;
init_per_testcase(_TC, Config) ->
    Config.

end_per_testcase(TC, Config) when TC =:= malloc; TC =:= delayed_dealloc_kill ->
    application:unset_env(sqlite, enif_alloc),
    ensure_unload(sqlite),
    Config;
end_per_testcase(_TC, Config) ->
    Config.

all() ->
    [basic, bind_step, shared, shared_cache, interrupt, crash, types, close, status,
        enif_alloc, prepared, errors, monitor, concurrency, race_close_prepare,
        delayed_dealloc_kill, malloc, backup, backup_sanity].

%%-------------------------------------------------------------------
%% Convenience functions

ensure_unload(Mod) ->
    erlang:module_loaded(Mod) andalso
        begin
            [garbage_collect(Pid) || Pid <- processes()], %% must GC all resource types of the library
            true = code:delete(Mod),
            code:purge(Mod)
        end,
    ?assertNot(erlang:module_loaded(Mod)).

create_blob_db(BlobCount) ->
    Src = sqlite:open("", #{flags => [memory]}),
    sqlite:query(Src, "CREATE TABLE large (blob BLOB)"),
    [] = sqlite:query(Src, "BEGIN;"),
    [[] = sqlite:query(Src, "INSERT INTO large VALUES(randomblob(4072));") || _ <- lists:seq(1, BlobCount)],
    [] = sqlite:query(Src, "COMMIT;"),
    Src.

start_backup() ->
    Src = create_blob_db(8),
    Dst = sqlite:open("", #{flags => [memory]}),
    %% start the backup in a separate process, to keep this one for tests
    Control = self(),
    Backup = spawn_link(
        fun () ->
            try
                Return = sqlite:backup(Src, Dst, #{step => 1, progress =>
                    fun (_Remain, _Total) ->
                        Control ! in_backup,
                        receive {continue, Ret} -> Ret end
                    end}),
                Control ! {return, Return}
            catch
                Class:Reason:Stack ->
                    Control ! {exception, Class, Reason, Stack}
            end
        end),
    receive in_backup -> ok end,
    {Src, Dst, Backup}.

continue_backup(Backup) ->
    Backup ! {continue, ok},
    receive Result -> Result end.

%% extra assertion macro that verifies extended error information
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
    Conn = sqlite:open("", #{flags => [memory]}),
    [] = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val TEXT) STRICT"),
    %% create a table with key -> value columns
    %% insert 1 => str
    [] = sqlite:query(Conn, "INSERT INTO kv (key, val) VALUES (1, 'str')"),
    Unicode = "юникод",
    [] = sqlite:query(Conn, "INSERT INTO kv (key, val) VALUES (?1, ?2)", [2, unicode:characters_to_binary(Unicode)]),
    ?assertEqual(2, sqlite:get_last_insert_rowid(Conn)),
    %% select
    [{1, <<"str">>}, {2, Encoded}] = sqlite:query(Conn, "SELECT * from kv ORDER BY key"),
    ?assertEqual(Unicode, unicode:characters_to_list(Encoded)),
    [] = sqlite:query(Conn, "SELECT * from kv WHERE val = 'h'").


shared() ->
    [{doc, "Test file database shared between two connections"}].

shared(Config) when is_list(Config) ->
    File = filename:join(proplists:get_value(priv_dir, Config), ?FUNCTION_NAME),
    Count = 4,
    Conns = [{Seq, sqlite:open(File, #{mode => read_write_create})} || Seq <- lists:seq(1, Count)],
    {1, C1} = hd(Conns),
    [] = sqlite:query(C1, "CREATE TABLE kv (key INTEGER PRIMARY KEY AUTOINCREMENT, val INTEGER) STRICT"),
    [sqlite:query(Conn, "INSERT INTO kv (val) VALUES (?1)", [Seq]) || {Seq, Conn} <- Conns],
    Sel = [sqlite:query(Conn, "SELECT val FROM kv ORDER BY val") || {_, Conn} <- Conns],
    %% must be the same
    [Sorted] = lists:usort(Sel),
    ?assertEqual([{S} || S <- lists:seq(1, Count)], Sorted).

shared_cache() ->
    [{doc, "Tests different open modes"}].

shared_cache(Config) when is_list(Config) ->
    Db1 = sqlite:open("file::mem:", #{flags => [shared, memory, uri]}),
    Db2 = sqlite:open("file::mem:", #{flags => [shared, memory, uri]}),
    [] = sqlite:query(Db1, "CREATE TABLE kv (key INTEGER PRIMARY KEY AUTOINCREMENT, val INTEGER)"),
    [] = sqlite:query(Db1, "INSERT INTO kv (val) VALUES (?1)", [1]),
    ?assertEqual([{1}], sqlite:query(Db2, "SELECT val FROM kv ORDER BY val")),
    sqlite:close(Db1), sqlite:close(Db2).

types(Config) when is_list(Config) ->
    Conn = sqlite:open("", #{flags => [memory]}),
    [] = sqlite:query(Conn, "CREATE TABLE types (t1 TEXT, b1 BLOB, i1 INTEGER, f1 REAL) STRICT"),
    %% TEXT storage class
    PreparedText = sqlite:prepare(Conn, "INSERT INTO types (t1) VALUES (?1)"),
    [] = sqlite:execute(PreparedText, [<<"TXTBIN">>]), %% accept a binary
    [] = sqlite:execute(PreparedText, ["TXTSTR"]), %% accept a string
    [] = sqlite:execute(PreparedText, [["T", ["XT"], <<"IO">>]]), %% accept an iolist
    %% get that back as a binary
    [{<<"TXTBIN">>}, {<<"TXTIO">>}, {<<"TXTSTR">>}] =
        sqlite:query(Conn, "SELECT t1 FROM types WHERE t1 IS NOT NULL ORDER BY t1"),
    %% test a wrapper turning binaries back to string
    %%
    %% BLOB storage class
    [] = sqlite:query(Conn, "INSERT INTO types (b1) VALUES (?1)", [{blob, <<1,2,3,4,5,6>>}]),
    [{<<1,2,3,4,5,6>>}] = sqlite:query(Conn, "SELECT b1 FROM types WHERE b1 IS NOT NULL"),
    %% INTEGER storage class
    PreparedInt = sqlite:prepare(Conn, "INSERT INTO types (i1) VALUES ($1)"),
    [] = sqlite:execute(PreparedInt, [576460752303423489]), %% bigint but fits
    [] = sqlite:execute(PreparedInt, [-576460752303423490]), %% bigint but fits
    [] = sqlite:execute(PreparedInt, [-576460752303423489]), %% small int
    [] = sqlite:execute(PreparedInt, [576460752303423488]), %% small int
    [{-576460752303423490}, {-576460752303423489}, {576460752303423488}, {576460752303423489}] =
        sqlite:query(Conn, "SELECT i1 FROM types WHERE i1 IS NOT NULL ORDER BY i1"),
    ?assertExtended(error, badarg,
        #{cause => #{2 => <<"bignum not supported">>, position => 1, general => <<"failed to bind parameter">>}},
        sqlite:execute(PreparedInt, [18446744073709551615])),
    %% type error takes precedence over invalid column error
    ?assertExtended(error, badarg,
        #{cause => #{2 => <<"bignum not supported">>, position => 2, general => <<"failed to bind parameter">>}},
        sqlite:execute(PreparedInt, #{2 => 18446744073709551615})),
    %% FLOAT storage class
    PreparedFloat = sqlite:prepare(Conn, "INSERT INTO types (f1) VALUES ($1)"),
    Pi = math:pi(),
    NegativePi = -Pi,
    [] = sqlite:execute(PreparedFloat, [Pi]),
    [] = sqlite:execute(PreparedFloat, [NegativePi]),
    [{NegativePi}, {Pi}] = sqlite:query(Conn, "SELECT f1 FROM types WHERE f1 IS NOT NULL ORDER BY f1"),
    %% null (undefined)
    %% atom
    Unsupported = #{cause => #{2 => <<"unsupported type">>, position => 1, general => <<"failed to bind parameter">>}},
    %% reference/fun/pid/port/THING
    ?assertExtended(error, badarg, Unsupported, sqlite:execute(PreparedInt, [self()])),
    %% tuple/record
    ?assertExtended(error, badarg, Unsupported, sqlite:execute(PreparedInt, [{}])),
    %% map (JSON?)
    ?assertExtended(error, badarg, Unsupported, sqlite:execute(PreparedInt, [#{}])),
    %% list is a string, but not a list of large numbers
    ?assertExtended(error, badarg, Unsupported, sqlite:execute(PreparedInt, [[12345]])).

status(Config) when is_list(Config) ->
    Conn = sqlite:open("", #{flags => [memory]}),
    [] = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val TEXT) STRICT"),
    Prepared = sqlite:prepare(Conn, "INSERT INTO kv (key, val) VALUES (1, ?1)"),
    %% sanity check, that fields are present, but not beyond that. Should are real assertions here
    #{deferred_fks := 0, statement := _,  lookaside_memory := Lookaside,
        pager_cache_memory := Cache} = sqlite:status(Conn),
    #{used := Used, shared := _Shared, hit := Hit, miss := _Miss, write := _Write, spill := _Spill} = Cache,
    #{used := _, max := _Max, hit := _, miss_size := _, miss_full := _} = Lookaside,
    ?assert(Used > 0, {used, Used}),
    ?assert(Hit > 0, {hit, Hit}),
    %% prepared statement stats
    #{fullscan_step := 0, sort := 0, autoindex := 0, vm_step := 0, reprepare := 0,
        run := 0, memory_used := _} = sqlite:info(Prepared),
    %%  system stats, skip page_cache check for not all versions have it
    #{memory_used := {_, _}, page_cache := {_, _, _, _, _}, malloc := {_, _, _},
        version := _Vsn} = sqlite:system_info().

enif_alloc() ->
    [{doc, "Tests that enif_alloc is used for SQLite allocations"}].

enif_alloc(Config) when is_list(Config) ->
    Conn = sqlite:open("", #{flags => [memory]}),
    [] = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val TEXT) STRICT"),
    Bin = iolist_to_binary([<<"1234567890ABCDEF">> || _ <- lists:seq(1, 128)]),
    Repeats = 16,
    BinSize = byte_size(Bin) * Repeats,
    Statement = sqlite:prepare(Conn, "INSERT INTO kv (key, val) VALUES (?1, ?2)"),
    %% test that erlang:memory() reflects changes when large allocations are done
    Before = erlang:memory(system),
    [[] = sqlite:execute(Statement, [Seq, Bin]) || Seq <- lists:seq(1, Repeats)],
    After = erlang:memory(system),
    Diff = After - Before,
    %% system memory should grow ~2x of the binding size
    ?assert(Diff > BinSize andalso Diff < (3 * BinSize), {difference, Diff, byte_size, BinSize}),
    %% finalise the statement - release some sqlite memory (but keep some of NIF resources)
    ok = sqlite:finish(Statement),
    Diff2 = erlang:memory(system) - After,
    %% just a bit of slack
    ?assert(Diff2 < 0, {not_final, Diff2}).

malloc() ->
    [{doc, "Tests that malloc is used for SQLite allocations"}].

malloc(Config) when is_list(Config) ->
    application:set_env(sqlite, enif_alloc, false),
    {module, sqlite} = code:load_file(sqlite),
    MemBefore = erlang:memory(system),
    Preps = alloc_some(),
    MemDiff = erlang:memory(system) - MemBefore,
    KeepRefs = tuple_size(Preps), %% only to avoid GC-ing "Preps"
    %% 200 prepared statements take > 100k, so if we test for 64k...
    ?assert(MemDiff < 64 * 1024, {unexpected_allocation, MemDiff}),
    ?assert(KeepRefs =:= 2).

prepared(Config) when is_list(Config) ->
    Conn = sqlite:open("", #{flags => [memory]}),
    [] = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val TEXT) STRICT"),
    PreparedInsert = sqlite:prepare(Conn, "INSERT INTO kv (key, val) VALUES (?1, ?2)"),
    [] = sqlite:execute(PreparedInsert, [1, "string"]),
    PreparedSel = sqlite:prepare(Conn, "SELECT * FROM kv WHERE key = ?1"),
    [{1, <<"string">>}] = sqlite:execute(PreparedSel, [1]),
    %% test statement into
    ?assertEqual([{<<"key">>,<<"INTEGER">>},{<<"val">>,<<"TEXT">>}], sqlite:describe(PreparedSel)).

named_parameters() ->
    [{doc, "Tests that named parameters map is accepted"}].

named_parameters(Config) when is_list(Config) ->
    Conn = sqlite:open("", #{flags => [memory]}),
    [] = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER, val TEXT) STRICT"),
    Prep = sqlite:prepare(Conn, "INSERT INTO kv (key, val) VALUES (:key, :value)"),
    %% test extended errors
    ?assertExtended(error, badarg, #{cause => #{2 => <<"parameter not found">>, general => <<":wrong">>}},
        sqlite:bind(Prep, #{wrong => 1})),
    ?assertExtended(error, {sqlite_error,25}, #{cause => #{2 => <<"column index out of range">>, position => 4,
        general => <<"failed to bind parameter">>}}, sqlite:bind(Prep, #{4 => "err"})),
    %%
    [] = sqlite:execute(Prep, #{key => 1, value => "text"}),
    [] = sqlite:execute(Prep, #{key => 2, value => "text2"}),
    [{1, <<"text">>}] = sqlite:query(Conn, "SELECT * FROM kv WHERE key = :col", #{<<"col">> => 1}),
    %% now try partial bindings - with some bits left from previous run
    ok = sqlite:reset(Prep),
    ok = sqlite:clear(Prep),
    ok = sqlite:bind(Prep, #{value => "two"}), %% key is unbound
    done = sqlite:step(Prep),
    ok = sqlite:reset(Prep),
    %% try using positional syntax for map binding
    ok = sqlite:bind(Prep, #{1 => 123}), %% now both key and value are bound
    done = sqlite:step(Prep),
    %% ensure we have all expected rows
    ?assertEqual([{1, <<"text">>}, {2, <<"text2">>}, {undefined, <<"two">>}, {123, <<"two">>}],
        sqlite:query(Conn, "SELECT * from kv")).

bind_step() ->
    [{doc, "Tests bind/2 and step/1,2"}].

bind_step(Config) when is_list(Config) ->
    Conn = sqlite:open("", #{flags => [memory]}),
    [] = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT) STRICT"),
    PreparedInsert = sqlite:prepare(Conn, "INSERT INTO kv (val) VALUES (?1)"),
    ok = sqlite:bind(PreparedInsert, ["str"]),
    done = sqlite:step(PreparedInsert),
    done = sqlite:step(PreparedInsert), %% sqlite allows that, and inserts another row
    %% insert & select multiple rows
    PreparedMultiInsert = sqlite:prepare(Conn, "INSERT INTO kv (val) VALUES (?1), (?), (?)"),
    ok = sqlite:bind(PreparedMultiInsert, ["2", "3", "4"]),
    {done, []} = sqlite:step(PreparedMultiInsert, 10),
    %% reset and step again
    ok = sqlite:reset(PreparedMultiInsert),
    done = sqlite:step(PreparedMultiInsert), %% that's one statement, after all
    %% clear bindings
    ok = sqlite:clear(PreparedMultiInsert),
    %% do not bind anything, but run the statement, which inserts 3 more rows with undefined value
    done = sqlite:step(PreparedMultiInsert),
    %% select
    PreparedSelect = sqlite:prepare(Conn, "SELECT * FROM kv ORDER BY key"),
    [{1, <<"str">>}, {2, <<"str">>}, {3, <<"2">>}, {4, <<"3">>}] = sqlite:step(PreparedSelect, 4),
    [{5, <<"4">>}, {6, <<"2">>}, {7, <<"3">>}, {8, <<"4">>}] = sqlite:step(PreparedSelect, 4),
    {done, [{9, undefined}, {10, undefined}, {11, undefined}]} = sqlite:step(PreparedSelect, 10),
    sqlite:close(Conn).

monitor(Config) when is_list(Config) ->
    Conn = sqlite:open("", #{flags => [memory]}),
    [] = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val TEXT) STRICT"),
    Ref = sqlite:monitor(Conn),
    %% verify monitoring works
    [] = sqlite:query(Conn, "INSERT INTO kv (key, val) VALUES (1, 'str')"),
    receive
        {Ref, insert, <<"main">>, <<"kv">>, _RowId} -> ok
    after 10 -> ?assert(false, "monitoring did not work, expected message is not received")
    end,
    %% test demonitoring
    sqlite:demonitor(Ref),
    [] = sqlite:query(Conn, "INSERT INTO kv (key, val) VALUES (2, 'str')"),
    receive Unexpected2 -> ?assert(false, {unexpected, Unexpected2})
    after 10 -> ok
    end.

close(Config) when is_list(Config) ->
    Conn = sqlite:open("", #{flags => [memory]}),
    [] = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val TEXT) STRICT"),
    ok = sqlite:close(Conn),
    ?assertExtended(error, badarg, #{cause => #{1 => <<"connection closed">>}},
        sqlite:prepare(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val TEXT) STRICT")),
    ?assertExtended(error, badarg, #{cause => #{1 => <<"connection already closed">>}}, sqlite:close(Conn)).


interrupt() ->
    [{doc, "Tests that interrupt works, and hopefully not crashing the VM"}].

interrupt(Config) when is_list(Config) ->
    Conn = sqlite:open("", #{flags => [memory]}),
    [] = sqlite:query(Conn, "CREATE TABLE any (val NOT NULL)"),
    %% now do the test
    Control = self(),
    _Long = spawn_link(
        fun () ->
            Result =
                try
                    sqlite:query(Conn, "WITH RECURSIVE
                    for(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM for WHERE i < 10000000)
                    INSERT INTO any SELECT i FROM for")
                catch
                    error:Reason ->
                        Reason
                end,
            Control ! {long, Result, os:system_time(millisecond)}
        end),
    timer:sleep(10), %% enough to get the Long process running
    %% close will hang until the interrupt comes, so spawn an extra process for it
    _Close = spawn_link(
        fun () ->
            sqlite:close(Conn),
            Control ! {close, os:system_time(millisecond)}
        end),
    %% interrupt in the test process after 100 ms
    timer:sleep(100),
    ok = sqlite:interrupt(Conn),
   %% get the interrupted & closing time (the latter must be greater)
    InterruptedAt = receive {long, Result, IntTime} -> ?assertEqual({sqlite_error, 9}, Result), IntTime end,
    %% get the closing time
    ClosedAt = receive {close, CloseTime} -> CloseTime end,
    ?assert(ClosedAt >= InterruptedAt, {interrupted, InterruptedAt, closed, ClosedAt, diff, ClosedAt - InterruptedAt}).

crash(Config) when is_list(Config) ->
    %% ensures that even after GC prepared statement is working
    Conn = sqlite:open("", #{flags => [memory]}),
    Prepared = sqlite:prepare(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val TEXT) STRICT"),
    sqlite:close(Conn),
    erlang:garbage_collect(),
    attempt_crash(Prepared).

attempt_crash(Prepared) ->
    ?assertExtended(error, badarg, #{cause => #{1 => <<"connection closed">>}},
        sqlite:execute(Prepared, [<<"1">>])).

delayed_dealloc_kill(Config) when is_list(Config) ->
    %% delete && purge the sqlite NIF module
    erlang:module_loaded(sqlite) andalso begin
        true = code:delete(sqlite),
        %% ensure that code purge kills the delayed_dealloc process
        Begin = processes(),
        code:purge(sqlite),
        WithoutPurger = Begin -- processes(),
        ?assert(length(WithoutPurger) =:= 1, WithoutPurger)
        end,
    %% ensure it's not loaded
    ?assertNot(erlang:module_loaded(sqlite)),
    Before = processes(),
    %% load implicitly, see the purger process there
    {module, sqlite} = code:load_file(sqlite),
    [Purger] = processes() -- Before,
    %% kill the purger
    MRef = erlang:monitor(process, Purger),
    erlang:exit(Purger, kill),
    receive {'DOWN', MRef, process, Purger, _Reason} -> ok end,
    %% trigger delayed dealloc
    MemBefore = erlang:memory(system),
    alloc_some(),
    erlang:garbage_collect(),  %% GC references to prepared statements and connection
    timer:sleep(500), %% deallocation is delayed, maybe think of better way to ensure cleanup?
    %% TODO: ensure the error is logged, but how? redirecting stderr?
    %% ensure that actual resource was freed
    MemDiff = erlang:memory(system) - MemBefore,
    ?assert(MemDiff < 10 * 1024, {memory_leak, MemDiff}).

alloc_some() ->
    Conn = sqlite:open("", #{flags => [memory]}),
    Preps = [sqlite:prepare(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val STRING)") || _ <- lists:seq(1, 200)],
    {Conn, Preps}.

backup(Config) when is_list(Config) ->
    Src = create_blob_db(8),
    %% do the basic backup
    Dst = sqlite:open("", #{flags => [memory]}),
    ok = sqlite:backup(Src, Dst),
    ?assertEqual([{8}], sqlite:query(Dst, "SELECT COUNT(*) FROM large")),
    %% do the fancier backup, with progress tracking, step setting and exception
    %% aborting the backup
    Dst2 = sqlite:open("", #{flags => [memory]}),
    try sqlite:backup(Src, Dst2, #{from => "main", step => 2, progress =>
        fun (Rem, _Total) when Rem > 6 -> ok;
            (Rem, _Total) when Rem > 4 -> {ok, 1};
            (Rem, Total) -> throw({expected, Rem, Total})
        end})
    catch
        throw:{expected, Remaining, Total} ->
            ?assertEqual(4, Remaining),
            ?assert(Total > 0 andalso Total > Remaining)
    end.

backup_sanity() ->
    [{doc, ""}].

backup_sanity(Config) when is_list(Config) ->
    {Src0, Dst0, Backup0} = start_backup(),
    sqlite:close(Src0), %% must force-close the backup (causing backup to abort)
    {exception, error, badarg, [{sqlite, _, _, Ext} | _]} = continue_backup(Backup0),
    ?assertEqual(#{1 => <<"backup aborted (connection closed)">>},
        maps:get(cause, proplists:get_value(error_info, Ext))),
    sqlite:close(Dst0),
    {Src1, Dst1, Backup1} = start_backup(),
    %% ensure that destination is locked when backup is in progress
    ?assertExtended(error, badarg, #{cause => #{1 => <<"connection busy (backup destination)">>}},
        sqlite:query(Dst1, "SELECT * from large")),
    %% check backup destination interaction with close/1
    sqlite:close(Dst1),
    sqlite:close(Src1),
    {exception, error, badarg, [{sqlite, _, _, Ext} | _]} = continue_backup(Backup1).

errors(Config) when is_list(Config) ->
    %% test EPP-54 extended error information
    ?assertExtended(error, badarg, #{cause => #{2 => <<"not a map">>}}, sqlite:open("", 123)),
    ?assertExtended(error, badarg, #{cause => #{2 => <<"unsupported mode">>}}, sqlite:open("", #{mode => invalid})),
    ?assertExtended(error, {sqlite_error, 14}, #{cause =>
        #{reason => <<"bad parameter or other API misuse">>, general => <<"unable to open database file">>}},
        sqlite:open("not_exist", #{mode => read_only})),
    %% need a statement for testing
    Db = sqlite:open("", #{flags => [memory]}),
    [] = sqlite:query(Db, "CREATE TABLE kv (key INTEGER PRIMARY KEY, val STRING)"),
    %% non-existent table
    ?assertExtended(error, {sqlite_error, 1},
        #{cause => #{2 => <<"no such table: non_existing">>, general => <<"SQL logic error">>}},
        sqlite:query(Db, "SELECT * from non_existing")),
    %% errors while preparing query
    ?assertExtended(error, {sqlite_error, 1}, #{cause =>
        #{2 => <<"near \"WHERE\": syntax error">>,position => 30, general => <<"SQL logic error">>}},
        sqlite:query(Db, "SELECT * from kv ORDER BY key WHERE key = ?", [1])),
    ?assertExtended(error, badarg, #{cause => #{3 => <<"invalid persistent value">>}},
        sqlite:prepare(Db, "SELECT * from kv ORDER BY key", #{persistent => maybe})),
    %% extra binding when not expected
    ?assertExtended(error, {sqlite_error, 25}, #{cause =>
        #{3 => <<"column index out of range">>,position => 2, general => <<"column index out of range">>}},
        sqlite:query(Db, "INSERT INTO kv (key) VALUES ($1)", [1, "1", 1])),
    %% not enough bindings
    ?assertExtended(error, badarg, #{cause => #{2 => <<"not enough parameters">>}},
        sqlite:query(Db, "INSERT INTO kv (key, val) VALUES ($1, $2)", [1])),
    sqlite:close(Db).

concurrency() ->
    [{doc, "Tests that concurrently opening, closing, and preparing does not crash"},
        {timetrap, {seconds, 60}}].

concurrency(Config) when is_list(Config) ->
    Concurrency = erlang:system_info(dirty_io_schedulers),
    FileName = filename:join(proplists:get_value(priv_dir, Config), "db.bin"),
    %% create a DB with schema
    Src = sqlite:open(FileName, #{busy_timeout => 60000}),
    [] = sqlite:query(Src, "CREATE TABLE kv (key INTEGER PRIMARY KEY AUTOINCREMENT, val INTEGER)"),
    %% use the connection by many workers
    Count = 500,
    StartOpt = #{control => self(), filename => FileName},
    Workers = [erlang:spawn_link(fun() -> worker(Count, rand:seed(exrop, {Seq, 0, 0}), Src, StartOpt) end)
        || Seq <- lists:seq(1, Concurrency)],
    [W ! {workers, Workers} || W <- Workers],
    %% wait for all of them to complete (with no error, otherwise the link crashes test runner)
    progress(Count * Concurrency, maps:from_list([{W, Count} || W <- Workers])).

progress(0, WP) ->
    Monitors = [{W, erlang:monitor(process, W)} || W <- maps:keys(WP)],
    [receive {'DOWN', Mon, process, Pid, Reason} -> Reason end || {Pid, Mon} <- Monitors];
progress(Total, WP) ->
    receive
        {progress, Worker, Left} ->
            #{Worker := Before} = WP,
            ?assert(Left =< Before, {left, Left, before, Before}),
            NewTotal = Total - (Before - Left),
            (Total div 1000) > (NewTotal div 1000) andalso
                io:format(user, " -> ~b", [NewTotal]),
            progress(NewTotal, WP#{Worker => Left})
    end.

worker(Count, Seed, Src, StartOpt) ->
    receive
        {workers, Workers} ->
            worker(Count, Seed, Src, [], StartOpt#{workers => Workers})
    end.

%% This is a mini-property based test, running random actions
worker(0, _Seed, _Db, _Statements, #{control := Control}) ->
    Control ! {progress, self(), 0};
worker(Count, Seed, Db, Statements, #{control := Control} = StartOpt) ->
    {Next, NewSeed} = rand:uniform_s(51, Seed),
    Control ! {progress, self(), Count},
    {NewCount, NS, NDb, NST} =
        case Next of
            1 ->
                try
                    ok = sqlite:close(Db),
                    #{filename := FileName, workers := Workers} = StartOpt,
                    NewDb = sqlite:open(FileName, #{busy_timeout => 60000, mode => read_write}),
                    %% notify all other workers
                    [W ! {new_db, NewDb} || W <- Workers, W =/= self()],
                    {Count - 1, NewSeed, NewDb, []}
                catch
                    error:badarg ->
                        {Count, Seed, receive_db(), []}
                end;
            2 ->
                Dst = sqlite:open("", #{flags => [memory]}),
                try sqlite:backup(Db, Dst),
                    sqlite:close(Dst),
                    {Count - 1, NewSeed, Db, Statements}
                catch
                    error:badarg ->
                        {Count, Seed, receive_db(), []}
                end;
            NewStatement when NewStatement < 12 ->
                try
                    Prep = sqlite:prepare(Db, "INSERT INTO kv (val) VALUES (?1)", #{persistent => true}),
                    {Count - 1, NewSeed, Db, [Prep | Statements]}
                catch
                    error:badarg ->
                        {Count, Seed, receive_db(), []}
                end;
            DelStatement when DelStatement < 22, length(Statements) > 0 ->
                NewStmts = lists:droplast(Statements),
                {Count - 1, NewSeed, Db, NewStmts};
            RunStatement when RunStatement < 52, length(Statements) > 0 ->
                try sqlite:execute(hd(Statements), [1]),
                    {Count - 1, NewSeed, Db, Statements}
                catch
                    error:badarg ->
                        {Count, Seed, receive_db(), []}
                end;
            _RunStatement ->
                %% skipping a step
                {Count, NewSeed, Db, Statements}
        end,
    worker(NewCount, NS, NDb, NST, StartOpt).

receive_db() ->
    receive
        {new_db, NewDb} ->
            NewDb
    end.

race_close_prepare() ->
    [{doc, "Tests that closing the connection while preparing the statement does not crash or leak"},
        {timetrap, {seconds, 60}}].

race_close_prepare(Config) when is_list(Config) ->
    do_race(5000).

do_race(0) -> ok;
do_race(Count) ->
    Conn = sqlite:open("", #{flags => [memory]}),
    [] = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY AUTOINCREMENT, val INTEGER)"),
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
    try sqlite:prepare(Conn, <<"SELECT * FROM kv WHERE key=?1">>), do_prepare(Count - 1, Conn)
    catch error:badarg -> ok end.
