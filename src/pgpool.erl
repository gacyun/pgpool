-module(pgpool).
-behaviour(application).
-behaviour(supervisor).

-export([start/0, stop/0, start/2, stop/1, init/1, squery/2, equery/3, equery/4, execute_batch/2, execute_batch/3, status/1]).


% -define(DEBUG_QUERY_LEVEL, io).
% -define(DEBUG_QUERY_LEVEL, error).

-ifdef(DEBUG_QUERY_LEVEL).
	-define(DEBUG_QUERY_NOW, DEBUG_QUERY_NOW = erlang:now()).
	-define(DEBUG_QUERY(Format, Args),
		case ?DEBUG_QUERY_LEVEL of
			io ->
				io:format("[PGPOOL]:~-2b [~8b] " ++ Format ++ "~n",
					[?LINE, timer:now_diff(erlang:now(), DEBUG_QUERY_NOW) | Args]);
			_ ->
				log4erl:log(?DEBUG_QUERY_LEVEL, "[PGPOOL_DEBUG]:~b [~b] " ++ Format,
				[?LINE, timer:now_diff(erlang:now(), DEBUG_QUERY_NOW) | Args])
		end).
-else.
	-define(DEBUG_QUERY_NOW, ok).
	-define(DEBUG_QUERY(Format, Args), ok).
-endif.


start() -> application:start(?MODULE).
stop()	-> application:stop(?MODULE).

start(_Type, _Args) ->
	supervisor:start_link({local, pgpool_sup}, ?MODULE, []).
stop(_State) -> ok.

init([]) ->
	{ok, Pools} = application:get_env(pgpool, pools),
	Fun = fun
		({PoolName, Size, DBUrl}) ->
			Args = [{name, {local, PoolName}},
				{worker_module, pgpool_worker},
				{size, Size},
				{db_url, DBUrl}],
				{PoolName, {poolboy, start_link, [Args]},
					permanent, 5000, worker, [poolboy]};
		({PoolName, Size, Overflow, DBUrl}) ->
			Args = [{name, {local, PoolName}},
				{worker_module, pgpool_worker},
				{size, Size},
				{max_overflow, Overflow},
				{db_url, DBUrl}],
			{PoolName, {poolboy, start_link, [Args]},
					permanent, 5000, worker, [poolboy]}
	end,
	PoolSpecs = lists:map(Fun, Pools),
	{ok, {{one_for_one, 10000, 10}, PoolSpecs}}.

squery(PoolName, Sql) ->
	Worker = poolboy:checkout(PoolName),
	try
		R = gen_server:call(Worker, {squery, Sql}),
		ok = poolboy:checkin(PoolName, Worker),
		R
	catch
		exit:{timeout, {gen_server, call, [_Pid, {squery, SQL = <<"INSERT", _/binary>>}, _Timeout]}} ->
			io:format("INSERT too slow!! ~p~n", [SQL]),
			gen_server:cast(Worker, stop),
			{error, execution_timeout};
		exit:{timeout, {gen_server, call, [Pid, {squery, _SQL = <<"SELECT", _/binary>>}, _Timeout]}} ->
			exit(Pid, kill),
			{error, execution_timeout};
		Exit:Reason ->
			exit(Worker, kill),
			{Exit, Reason}
	end.

equery(PoolName, Stmt, Params) ->
	equery(PoolName, Stmt, Params, 5000).

equery(PoolName, Stmt, Params, Timeout) ->
	?DEBUG_QUERY_NOW,
	?DEBUG_QUERY("B ~p ~p", [self(), Stmt]), % before checkout
	Worker = poolboy:checkout(PoolName),
	?DEBUG_QUERY("O ~p ~p ~p", [Worker, Stmt, Params]), % after checkout
	try
		R = gen_server:call(Worker, {equery, Stmt, Params}, Timeout),
		ok = poolboy:checkin(PoolName, Worker),
		?DEBUG_QUERY("I ~p ~p", [Worker, Stmt]), % after checkin
		R
	catch
		exit:{timeout, {gen_server, call, [_Pid, {equery, SQL = <<"INSERT", _/binary>>, _Args}, _Timeout]}} ->
			io:format("INSERT too slow!! ~p~n", [SQL]),
			gen_server:cast(Worker, stop),
			{error, execution_timeout};
		exit:{timeout, {gen_server, call, [Pid, {equery, _SQL = <<"SELECT", _/binary>>, _Args}, _Timeout]}} ->
			exit(Pid, kill),
			{error, execution_timeout};
		Exit:Reason ->
			exit(Worker, kill),
			{Exit, Reason}
	end.

execute_batch(PoolName, Batch) ->
	execute_batch(PoolName, Batch, 5000).

execute_batch(PoolName, Batch, Timeout) ->
	?DEBUG_QUERY_NOW,
	?DEBUG_QUERY("B ~p ~p", [self()]), % before checkout
	Worker = poolboy:checkout(PoolName),
	?DEBUG_QUERY("O ~p ~p ~p", [Worker, Batch]), % after checkout
	try
		R = gen_server:call(Worker, {execute_batch, Batch}, Timeout),
		ok = poolboy:checkin(PoolName, Worker),
		?DEBUG_QUERY("I ~p ~p", [Worker, Batch]), % after checkin
		R
	catch
		exit:{timeout, {gen_server, call, [_Pid, {execute_batch, Batch}, _Timeout]}} ->
			io:format("INSERT too slow!! ~p~n", [Batch]),
			gen_server:cast(Worker, stop),
			{error, execution_timeout};
		exit:{timeout, {gen_server, call, [Pid, {execute_batch, _Batch}, _Timeout]}} ->
			exit(Pid, kill),
			{error, execution_timeout};
		Exit:Reason ->
			exit(Worker, kill),
			{Exit, Reason}
	end.


status(PoolName) ->
	case whereis(PoolName) of
		undefined ->
			{none, not_found};
		_ ->
			poolboy:status(PoolName)
	end.
