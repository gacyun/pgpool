-module(pgpool_worker).
-behaviour(gen_server).

-export([start_link/1, stop/0, init/1, handle_call/3, handle_cast/2,
	handle_info/2, terminate/2, code_change/3]).

-record(state, {conn}).

start_link(Args) -> gen_server:start_link(?MODULE, Args, []).
stop() -> gen_server:cast(?MODULE, stop).

init(Args) ->
	% process_flag(trap_exit, true),
	DB_URL = proplists:get_value(db_url, Args),
	{pg, Host, Username, Password, Opts} = pgpool_utils:parse_db_url(DB_URL),
	case epgsql:connect(Host, Username, Password, Opts) of
		{ok, Conn} ->
			{ok, #state{conn = Conn}};
		{error, Reason} ->
			{stop, {error, Reason}}
	end.

handle_call({parse, Name, Sql, Types}, _From, #state{conn=Conn}=State) ->
	{reply, epgsql:parse(Conn, Name, Sql, Types), State};

handle_call({squery, Sql}, _From, #state{conn=Conn}=State) ->
	{reply, epgsql:squery(Conn, Sql), State};

handle_call({equery, Stmt, Params}, _From, #state{conn=Conn}=State) ->
	{reply, epgsql:equery(Conn, Stmt, Params), State};

handle_call({execute_batch, Batch}, _From, #state{conn=Conn}=State) ->
	{reply, epgsql:execute_batch(Conn, Batch), State};


handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast(stop, State) ->
	{stop, shutdown, State};

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info({'EXIT', _, _}, State) ->
	{stop, shutdown, State};

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, #state{conn=Conn}) ->
	epgsql:close(Conn),
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
