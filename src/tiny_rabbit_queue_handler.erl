-module(tiny_rabbit_queue_handler).

-export([behaviour_info/1]).

%% @private
-spec behaviour_info(_)
    -> undefined | [{handle, 2} | {init, 1} | {terminate, 2}, ...].
behaviour_info(callbacks) ->
    [{init, 1}, {handle, 2}, {terminate, 2}];
behaviour_info(_Other) ->
    undefined.
