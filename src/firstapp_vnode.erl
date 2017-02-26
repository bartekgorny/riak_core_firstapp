-module(firstapp_vnode).
-behaviour(riak_core_vnode).
-include("firstapp.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-ignore_xref([
             start_vnode/1
             ]).

-record(state, {partition, signals}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    ?LOG({vnode_init, Partition}),
    {ok, #state { partition=Partition, signals=#{} }}.

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    ?LOG(ping),
    {reply, {pong, State#state.partition}, State};
handle_command({sig, Did, Data}, _Sender, State) ->
    Ss = State#state.signals,
    R = maps:get(Did, Ss, []),
    Nr = R ++ [Data],
    Ns = maps:put(Did, Nr, Ss),
    ?LOG(Did, Nr),
    {reply, gotit, State#state{signals=Ns}};
handle_command({get, Did}, _Sender, State) ->
    Rep = get_data(Did, State),
    {reply, Rep, State};
handle_command(Message, _Sender, State) ->
    ?LOG(unhandled_command, Message),
    {noreply, State}.

handle_handoff_command({get, Did}, _Sender, State) ->
    ?LOG(handoff_get, Did),
    Rep = get_data(Did, State),
    {reply, Rep, State};
handle_handoff_command(#riak_core_fold_req_v2{foldfun=FoldFun, acc0=Acc0}, _Sender, State) ->
    Acc1 = maps:fold(FoldFun, Acc0, State#state.signals),
    {reply, Acc1, State};
handle_handoff_command(Message, _Sender, State) ->
    ?LOG(handoff_cmd, Message),
    {_Type, _Fun, Acc, _, _} = Message,
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    ?LOG(handoff_starting),
    {true, State}.

handoff_cancelled(State) ->
    ?LOG(handoff_cancelled),
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    ?LOG(handoff_finished),
    {ok, State}.

handle_handoff_data(Data, #state{signals = Signals} = State) ->
    ?LOG(handoff_data, Data),
    {Key, Value} = binary_to_term(Data),
    NSig = maps:put(Key, Value, Signals),
    {reply, ok, State#state{signals = NSig}}.

encode_handoff_item(ObjectName, ObjectValue) ->
    ?LOG("encode_handoff", {ObjectName, ObjectValue}),
    term_to_binary({ObjectName, ObjectValue}).

is_empty(State) ->
    Ss = State#state.signals,
    {Ss == #{}, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ?LOG(vnode_terminate),
    ok.

get_data(Did, State) ->
    Ss = State#state.signals,
    R = maps:get(Did, Ss, []),
    ?LOG(Did, R),
    {ihave, R}.


%%{riak_core_fold_req_v2,#Fun<riak_core_handoff_sender.2.39179541>,
%%{ho_acc,0,ok,#Fun<riak_core_handoff_sender.15.39179541>,firstapp_vnode,<0.400.0>,#Port<0.425656>,
%%{1438665674247607560106752257205091097473808596992,1438665674247607560106752257205091097473808596992},
%%{ho_stats,{1486,487537,74606},undefined,0,0},gen_tcp,0,0,true,[],0,0,25,undefined,ownership,undefined,undefined},false,[]}
