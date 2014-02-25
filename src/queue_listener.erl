-module(queue_listener).

-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

%% API
-export([start_link/5]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {channel,
                handler,
                handler_state}).

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
start_link(Queue, Number, ConnectionTracker, Handler, HandlerOptions) ->
    lager:debug("start ~p", [Number]),
    gen_server:start_link(?MODULE, [Queue, Number, ConnectionTracker, Handler, HandlerOptions], []).

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
init([QueueStr, Number, ConnectionTracker, Handler, HandlerOptions]) ->
    Queue = list_to_binary(QueueStr),
    {ok, HandlerState} = Handler:init(HandlerOptions),
    lager:debug("Queue: ~p, Num: ~p", [Queue, Number]),
    {ok, Connection} = connection_tracker:get_connection(ConnectionTracker),
    lager:debug("Connection ~p", [Connection]),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    lager:debug("channel ~p", [Channel]),
    connection_tracker:register_worker(ConnectionTracker, self(), Channel),
    Declare = #'queue.declare'{queue = Queue, durable = true},
    #'queue.declare_ok'{} = amqp_channel:call(Channel, Declare),
    amqp_channel:call(Channel, #'basic.qos'{prefetch_count = 1}),
    Sub = #'basic.consume'{queue = Queue},
    #'basic.consume_ok'{} = amqp_channel:call(Channel, Sub),
    put(channel, Channel),
    put(connection, Connection),
    put(number, Number),
    put(queue, Queue),
    {ok, #state{channel = Channel, handler = Handler, handler_state = HandlerState}}.

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
handle_info(#'basic.consume_ok'{}, #state{channel = Channel} = State) ->
    lager:debug("Start consume ~p", [Channel]),
    {noreply, State};
    
handle_info({#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{payload = Payload}}, #state{channel = Channel, handler = Handler, handler_state = HandlerState} = State) ->
    lager:debug("receive message ~p. channel ~p", [Payload, Channel]),
    {ok, NewHandlerState} = Handler:handle(Payload, HandlerState),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
    {noreply, State#state{handler_state = NewHandlerState}};
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
