%%%-------------------------------------------------------------------
%%% @copyright (C) 2023 Maxim Fedorov
%%% Benchmarks - use for testing performance
-module(sqlite_bench_SUITE).
-author("maximfca@gmail.com").

%% Common Test API
-export([suite/0, all/0]).

%% Test cases
-export([basic/1, benchmark_prepared/1]).

-behaviour(ct_suite).

-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 600}}].

all() ->
    [basic, benchmark_prepared].

%%-------------------------------------------------------------------
%% Convenience functions

basic(Config) when is_list(Config) ->
    ok.

%% benchmark for prepared statements
-define(QUERY, <<"SELECT * FROM kv WHERE key=?1">>).
benchmark_prepared(Config) when is_list(Config) ->
    measure_one(fun bench_query/2, "query"),
    measure_one(fun bench_prep/2, "prepare every time"),
    measure_one(fun bench_one/2, "prepare once").

measure_one(FunTo, Kind) ->
    Conn = sqlite:open("", #{flags => [memory]}),
    [] = sqlite:query(Conn, "CREATE TABLE kv (key INTEGER PRIMARY KEY AUTOINCREMENT, val INTEGER)"),
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

