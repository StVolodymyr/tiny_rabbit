-module(tiny_rabbit).

-export([start_listeners/9]).

start_listeners(Host, Port, User, Pass, Vhost, Queue, ListenersCount, Handler, HandlerOptions) ->
    supervisor:start_child(tiny_rabbit_sup,
                                {{connection_sup, Queue},
                                 {connection_sup, start_link, [Host, Port, User, Pass, Vhost, Queue, ListenersCount, Handler, HandlerOptions]},
                                 permanent,
                                 5000,
                                 supervisor,
                                 [connection_sup]
                                }).
