-module(firstapp_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-include("firstapp.hrl").
%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case firstapp_sup:start_link() of
        {ok, Pid} ->
            initialise(),
            {ok, Pid};
        {error, {already_started, Pid}} ->
            initialise(),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

initialise() ->
    ?LOG(starting_app),
    ok = riak_core:register([{vnode_module, firstapp_vnode}]),
    ok = riak_core_ring_events:add_guarded_handler(firstapp_ring_event_handler, []),
    ok = riak_core_node_watcher_events:add_guarded_handler(firstapp_node_event_handler, []),
    ok = riak_core_node_watcher:service_up(firstapp, self()).

stop(_State) ->
    ok.
