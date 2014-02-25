-module(connection_sup).

-behaviour(supervisor).

%% API
-export([start_link/9]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     permanent, 5000, Type, [Mod]}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Host, Port, User, Pass, Vhost, Queue, ListenersCount, Handler, HandlerOptions) ->
    {ok, SupPid} = supervisor:start_link(?MODULE, []),
    {ok, ConnectionTrackerPid} = supervisor:start_child(SupPid,
                                                        {{connection_tracker, Queue},
                                                         {connection_tracker, start_link, [Host, Port, User, Pass, Vhost]},
                                                         permanent,
                                                         5000,
                                                         worker,
                                                         dynamic
                                                        }),
    supervisor:start_child(SupPid,
                           {{listener_sup, Queue},
                            {supervisor, start_link, [?MODULE, [{listener_sup, Queue, ListenersCount, ConnectionTrackerPid, Handler, HandlerOptions}]]},
                            permanent,
                            5000,
                            supervisor,
                            []
                           }),
    {ok, SupPid}.

child_spec(Queue, Num, ConnectionTrackerPid, Handler, HandlerOptions) ->
	{{listener, Num}, 
		{queue_listener, start_link, [Queue, Num, ConnectionTrackerPid, Handler, HandlerOptions]},
		permanent,
		5000,
		worker,
		[]
	}.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([{listener_sup, Queue, ListenersCount, ConnectionTrackerPid, Handler, HandlerOptions}]) ->
    Children = [ child_spec(Queue, Num, ConnectionTrackerPid, Handler, HandlerOptions) || Num <- lists:seq(1, ListenersCount) ],
    {ok, {{one_for_one, 5, 10}, Children}};
init([]) ->
    {ok, {{one_for_all, 0, 10}, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
