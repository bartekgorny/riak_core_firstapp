-module(firstapp).
-include("firstapp.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
         sig/2,
         get/1
        ]).

-ignore_xref([
              ping/0
             ]).

%% Public API

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    [Pref|_] = riak_core_apl:get_primary_apl(DocIdx, 2, firstapp),
    {IndexNode, _Type} = Pref,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, firstapp_vnode_master).

sig(Did, Data) ->
    DocIdx = riak_core_util:chash_key({<<"sig">>, Did}),
    [Pref|_] = riak_core_apl:get_primary_apl(DocIdx, 1, firstapp),
    {IndexNode, _Type} = Pref,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {sig, Did, Data}, firstapp_vnode_master).

get(Did) ->
    DocIdx = riak_core_util:chash_key({<<"sig">>, Did}),
    [Pref|_] = riak_core_apl:get_primary_apl(DocIdx, 1, firstapp),
    {IndexNode, _Type} = Pref,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {get, Did}, firstapp_vnode_master).
