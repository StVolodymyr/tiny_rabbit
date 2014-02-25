-module(connection_tracker).

-behaviour(gen_server).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%% API
-export([start_link/5]).
-export([get_connection/1, register_worker/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(worker_entry, {
        worker_pid,
        channel_pid
        }).


-record(state, {
        amqp_host,
        amqp_port,
        amqp_user,
        amqp_pass,
        amqp_vhost,
        connection,
        ets_id
        }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Host, Port, User, Pass, Vhost) ->
    gen_server:start_link(?MODULE, [Host, Port, User, Pass, Vhost], []).

get_connection(Pid) ->
    gen_server:call(Pid, get_connection).

register_worker(Pid, Worker, Channel) ->
    gen_server:call(Pid, {register_worker, {Worker, Channel}}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Host, Port, User, Pass, Vhost]) ->
    self() ! connect,
    EtsId = ets:new(?MODULE, [private, {keypos, #worker_entry.worker_pid}]),
    {ok, #state{ets_id = EtsId,
                amqp_host = Host,
                amqp_port = Port,
                amqp_user = list_to_binary(User),
                amqp_pass = list_to_binary(Pass),
                amqp_vhost = list_to_binary(Vhost)
               }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({register_worker, {Worker, Channel}}, _From, #state{ets_id = EtsId} = State) ->
    _WorkerMonitor = erlang:monitor(process, Worker),
    _ChannelMonitor = erlang:monitor(process, Channel),
    ets:insert(EtsId, #worker_entry{worker_pid = Worker, channel_pid = Channel}),
    Reply = ok,
    {reply, Reply, State};
handle_call(get_connection, _From, #state{connection = Connection} = State) ->
    Reply = {ok, Connection},
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(connect, #state{amqp_host = Host,
                            amqp_port = Port,
                            amqp_user = User,
                            amqp_pass = Pass,
                            amqp_vhost = VHost} = State) ->
    ConnectionOptions =   #amqp_params_network{host = Host,
                                               port = Port,
                                               username = User,
                                               password = Pass,
                                               virtual_host = VHost},

    {ok, Connection} = amqp_connection:start(ConnectionOptions),
    put(connection, Connection),
    lager:debug("Open new connection ~p", [Connection]),
    link(Connection),
    {noreply, State#state{connection = Connection}};

handle_info({'DOWN', _, process, WorkerOrChannel, _Reason}, #state{ets_id = EtsId} = State) ->
    lager:error("Process ~p is down", [WorkerOrChannel]),
    case ets:select(EtsId, ets:fun2ms(fun(#worker_entry{worker_pid = Worker, channel_pid = Channel}) when Worker == WorkerOrChannel -> Channel end)) of
        [] ->
            case ets:select(EtsId, ets:fun2ms(fun(#worker_entry{worker_pid = Worker, channel_pid = Channel}) when Channel == WorkerOrChannel -> Worker end)) of
                [] ->
                    ok;
                Workers ->
                    lager:error("Stop workers ~p", [Workers]),
                    [ Worker ! stop || Worker <- Workers ],
                    ets:select_delete(EtsId, ets:fun2ms(fun(#worker_entry{channel_pid = Channel}) when Channel == WorkerOrChannel -> true end)),
                    ok
            end;
        Channels ->
            lager:error("Close channel ~p", [Channels]),
            [ amqp_channel:close(Channel) || Channel <- Channels ],
            ets:select_delete(EtsId, ets:fun2ms(fun(#worker_entry{worker_pid = Worker}) when Worker == WorkerOrChannel -> true end)),
            ok
    end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
