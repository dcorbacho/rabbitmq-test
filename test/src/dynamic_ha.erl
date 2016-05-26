%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%
-module(dynamic_ha).

%% rabbit_tests:test_dynamic_mirroring() is a unit test which should
%% test the logic of what all the policies decide to do, so we don't
%% need to exhaustively test that here. What we need to test is that:
%%
%% * Going from non-mirrored to mirrored works and vice versa
%% * Changing policy can add / remove mirrors and change the master
%% * Adding a node will create a new mirror when there are not enough nodes
%%   for the policy
%% * Removing a node will not create a new mirror even if the policy
%%   logic wants it (since this gives us a good way to lose messages
%%   on cluster shutdown, by repeated failover to new nodes)
%%
%% The first two are change_policy, the last two are change_cluster

-compile(export_all).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(QNAME, <<"ha.test">>).
-define(POLICY, <<"^ha.test$">>). %% " emacs
-define(VHOST, <<"/">>).

-import(rabbit_test_util, [set_ha_policy/3, set_ha_policy/4,
                           clear_policy/2, a2b/1, publish/3, consume/3]).
-import(rabbit_misc, [pget/2]).

change_policy_with() -> cluster_abc.
change_policy([CfgA, _CfgB, _CfgC] = Cfgs) ->
    ACh = pget(channel, CfgA),
    [A, B, C] = [pget(node, Cfg) || Cfg <- Cfgs],

    %% When we first declare a queue with no policy, it's not HA.
    amqp_channel:call(ACh, #'queue.declare'{queue = ?QNAME}),
    assert_slaves(A, ?QNAME, {A, ''}),

    %% Give it policy "all", it becomes HA and gets all mirrors
    set_ha_policy(CfgA, ?POLICY, <<"all">>),
    assert_slaves(A, ?QNAME, {A, [B, C]}),

    %% Give it policy "nodes", it gets specific mirrors
    set_ha_policy(CfgA, ?POLICY, {<<"nodes">>, [a2b(A), a2b(B)]}),
    assert_slaves(A, ?QNAME, {A, [B]}),

    %% Now explicitly change the mirrors
    set_ha_policy(CfgA, ?POLICY, {<<"nodes">>, [a2b(A), a2b(C)]}),
    assert_slaves(A, ?QNAME, {A, [C]}, [{A, [B, C]}]),

    %% Clear the policy, and we go back to non-mirrored
    ok = clear_policy(CfgA, ?POLICY),
    assert_slaves(A, ?QNAME, {A, ''}),

    %% Test switching "away" from an unmirrored node
    set_ha_policy(CfgA, ?POLICY, {<<"nodes">>, [a2b(B), a2b(C)]}),
    assert_slaves(A, ?QNAME, {A, [B, C]}, [{A, [B]}, {A, [C]}]),

    ok.

change_cluster_with() -> cluster_abc.
change_cluster([CfgA, _CfgB, _CfgC] = CfgsABC) ->
    ACh = pget(channel, CfgA),
    [A, B, C] = [pget(node, Cfg) || Cfg <- CfgsABC],

    amqp_channel:call(ACh, #'queue.declare'{queue = ?QNAME}),
    assert_slaves(A, ?QNAME, {A, ''}),

    %% Give it policy exactly 4, it should mirror to all 3 nodes
    set_ha_policy(CfgA, ?POLICY, {<<"exactly">>, 4}),
    assert_slaves(A, ?QNAME, {A, [B, C]}),

    %% Add D and E, D joins in
    [CfgD, CfgE] = CfgsDE = rabbit_test_configs:start_nodes(CfgA, [d, e], 5675),
    D = pget(node, CfgD),
    E = pget(node, CfgE),
    rabbit_test_configs:add_to_cluster(CfgsABC, CfgsDE),
    assert_slaves(A, ?QNAME, {A, [B, C, D]}),

    %% Remove D, E joins in
    rabbit_test_configs:stop_node(CfgD),
    assert_slaves(A, ?QNAME, {A, [B, C, E]}),

    %% Clean up since we started this by hand
    rabbit_test_configs:stop_node(CfgE),
    ok.

rapid_change_with() -> cluster_abc.
rapid_change([CfgA, _CfgB, _CfgC]) ->
    ACh = pget(channel, CfgA),
    {_Pid, MRef} = spawn_monitor(
                     fun() ->
                             [rapid_amqp_ops(ACh, I) || I <- lists:seq(1, 100)]
                     end),
    rapid_loop(CfgA, MRef),
    ok.

rapid_amqp_ops(Ch, I) ->
    Payload = list_to_binary(integer_to_list(I)),
    amqp_channel:call(Ch, #'queue.declare'{queue = ?QNAME}),
    amqp_channel:cast(Ch, #'basic.publish'{exchange = <<"">>,
                                           routing_key = ?QNAME},
                      #amqp_msg{payload = Payload}),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue    = ?QNAME,
                                                no_ack   = true}, self()),
    receive #'basic.consume_ok'{} -> ok
    end,
    receive {#'basic.deliver'{}, #amqp_msg{payload = Payload}} ->
            ok
    end,
    amqp_channel:call(Ch, #'queue.delete'{queue = ?QNAME}).

rapid_loop(Cfg, MRef) ->
    receive
        {'DOWN', MRef, process, _Pid, normal} ->
            ok;
        {'DOWN', MRef, process, _Pid, Reason} ->
            exit({amqp_ops_died, Reason})
    after 0 ->
            set_ha_policy(Cfg, ?POLICY, <<"all">>),
            ok = clear_policy(Cfg, ?POLICY),
            rapid_loop(Cfg, MRef)
    end.

%% Vhost deletion needs to successfully tear down policies and queues
%% with policies. At least smoke-test that it doesn't blow up.
vhost_deletion_with() -> [cluster_ab, ha_policy_all].
vhost_deletion([CfgA, _CfgB]) ->
    ACh = pget(channel, CfgA),
    Node = pget(node, CfgA),
    amqp_channel:call(ACh, #'queue.declare'{queue = <<"test">>}),
    ok = rpc:call(Node, rabbit_vhost, delete, [<<"/">>]),
    ok.

promote_on_shutdown_with() -> cluster_ab.
promote_on_shutdown([CfgA, CfgB]) ->
    set_ha_policy(CfgA, <<"^ha.promote">>, <<"all">>,
                  [{<<"ha-promote-on-shutdown">>, <<"always">>}]),
    set_ha_policy(CfgA, <<"^ha.nopromote">>, <<"all">>),

    ACh = pget(channel, CfgA),
    [begin
         amqp_channel:call(ACh, #'queue.declare'{queue   = Q,
                                                 durable = true}),
         publish(ACh, Q, 10)
     end || Q <- [<<"ha.promote.test">>, <<"ha.nopromote.test">>]],
    rabbit_test_configs:restart_node(CfgB),
    CfgA1 = rabbit_test_configs:stop_node(CfgA),
    {_, BCh} =  rabbit_test_util:connect(CfgB),
    #'queue.declare_ok'{message_count = 0} = 
        amqp_channel:call(
          BCh, #'queue.declare'{queue   = <<"ha.promote.test">>,
                                durable = true}),
    ?assertExit(
       {{shutdown, {server_initiated_close, 404, _}}, _},
       amqp_channel:call(
         BCh, #'queue.declare'{queue   = <<"ha.nopromote.test">>,
                               durable = true})),
    CfgA2 = rabbit_test_configs:start_node(CfgA1),
    {_, ACh2} =  rabbit_test_util:connect(CfgA2),
    #'queue.declare_ok'{message_count = 10} =
        amqp_channel:call(
          ACh2, #'queue.declare'{queue   = <<"ha.nopromote.test">>,
                                 durable = true}),
    ok.

random_policy_with() -> cluster_abc.
random_policy([_CfgA, _CfgB, _CfgC] = Cfgs) ->
    run_proper(fun prop_random_policy/1, [Cfgs]).

%%----------------------------------------------------------------------------

assert_slaves(RPCNode, QName, Exp) ->
    assert_slaves(RPCNode, QName, Exp, []).

assert_slaves(RPCNode, QName, Exp, PermittedIntermediate) ->
    assert_slaves0(RPCNode, QName, Exp,
                  [{get(previous_exp_m_node), get(previous_exp_s_nodes)} |
                   PermittedIntermediate]).

assert_slaves0(RPCNode, QName, {ExpMNode, ExpSNodes}, PermittedIntermediate) ->
    Q = find_queue(QName, RPCNode),
    Pid = proplists:get_value(pid, Q),
    SPids = proplists:get_value(slave_pids, Q),
    ActMNode = node(Pid),
    ActSNodes = case SPids of
                    '' -> '';
                    _  -> [node(SPid) || SPid <- SPids]
                end,
    case ExpMNode =:= ActMNode andalso equal_list(ExpSNodes, ActSNodes) of
        false ->
            %% It's an async change, so if nothing has changed let's
            %% just wait - of course this means if something does not
            %% change when expected then we time out the test which is
            %% a bit tedious
            case [found || {PermMNode, PermSNodes} <- PermittedIntermediate,
                           PermMNode =:= ActMNode,
                           equal_list(PermSNodes, ActSNodes)] of
                [] -> io:format(user, "Expected ~p / ~p, got ~p / ~p~nat ~p~n",
                              [ExpMNode, ExpSNodes, ActMNode, ActSNodes,
                               get_stacktrace()]);
                _  -> timer:sleep(100),
                      assert_slaves0(RPCNode, QName, {ExpMNode, ExpSNodes},
                                     PermittedIntermediate)
            end;
        true ->
            put(previous_exp_m_node, ExpMNode),
            put(previous_exp_s_nodes, ExpSNodes),
            ok
    end.

equal_list('',    '')   -> true;
equal_list('',    _Act) -> false;
equal_list(_Exp,  '')   -> false;
equal_list([],    [])   -> true;
equal_list(_Exp,  [])   -> false;
equal_list([],    _Act) -> false;
equal_list([H|T], Act)  -> case lists:member(H, Act) of
                               true  -> equal_list(T, Act -- [H]);
                               false -> false
                           end.

find_queue(QName, RPCNode) ->
    Qs = rpc:call(RPCNode, rabbit_amqqueue, info_all, [?VHOST], infinity),
    case find_queue0(QName, Qs) of
        did_not_find_queue -> timer:sleep(100),
                              find_queue(QName, RPCNode);
        Q -> Q
    end.

find_queue0(QName, Qs) ->
    case [Q || Q <- Qs, proplists:get_value(name, Q) =:=
                   rabbit_misc:r(?VHOST, queue, QName)] of
        [R] -> R;
        []  -> did_not_find_queue
    end.

get_stacktrace() ->
    try
        throw(e)
    catch
        _:e ->
            erlang:get_stacktrace()
    end.

%%----------------------------------------------------------------------------
run_proper(Fun, Args) ->
    case proper:counterexample(erlang:apply(Fun, Args),
                               [{numtests, 25},
                                {on_output, fun(F, A) ->
                                                    io:format(user, F, A)
                                            end}]) of
        true ->
            true;
        Value ->
            exit(Value)
    end.

prop_random_policy([CfgA, _CfgB, _CfgC] = Cfgs) ->
    Nodes = [proplists:get_value(node, Cfg) || Cfg <- Cfgs],
    ?FORALL(
       Policies, non_empty(list(policy_gen(Nodes))),
       begin
           Ch = pget(channel, CfgA),
           amqp_channel:call(Ch, #'queue.declare'{queue = ?QNAME}),
           %% Add some load so mirrors can be busy synchronising
           publish(Ch, ?QNAME, 100000),
           %% Apply policies in parallel on all nodes
           apply_in_parallel(Cfgs, Policies),
           %% The last policy is the final state
           Last = lists:last(Policies),
           %% Give it some time to generate all internal notifications
           timer:sleep(2000),
           %% Ensure the owner/master is able to process a call request,
           %% which means that all pending casts have been processed.
           %% Use the information returned by owner/master to verify the
           %% test result
           Node = proplists:get_value(node, CfgA),
           Info = find_queue(?QNAME, Node),
           %% Gets owner/master
           Pid = proplists:get_value(pid, Info),
           FinalInfo = rpc:call(node(Pid), gen_server, call, [Pid, info], 5000),
           %% Check the result
           Result = verify_policy(Last, FinalInfo),
           %% Cleanup
           amqp_channel:call(Ch, #'queue.delete'{queue = ?QNAME}),
           _ = clear_policy(CfgA, ?POLICY),
           Result
       end).

apply_in_parallel(Cfgs, Policies) ->
    Self = self(),
    [spawn_link(fun() ->
                        [begin
                             apply_policy(Cfg, Policy)
                         end || Policy <- Policies],
                        Self ! parallel_task_done
                end) || Cfg <- Cfgs],
    [receive
         parallel_task_done ->
             ok
     end || _ <- Cfgs].

%% Proper generators
policy_gen(Nodes) ->
    %% Stop mirroring needs to be called often to trigger rabbitmq-server#803
    frequency([{3, undefined},
               {1, all},
               {1, {nodes, nodes_gen(Nodes)}},
               {1, {exactly, choose(1, 3)}}
              ]).

nodes_gen(Nodes) ->
    ?LET(List, non_empty(list(oneof(Nodes))),
         sets:to_list(sets:from_list(List))).

%% Checks
verify_policy(undefined, Info) ->
    %% If the queue is not mirrored, it returns ''
    '' == proplists:get_value(slave_pids, Info);
verify_policy(all, Info) ->
    2 == length(proplists:get_value(slave_pids, Info));
verify_policy({exactly, 1}, Info) ->
    %% If the queue is mirrored, it returns a list
    [] == proplists:get_value(slave_pids, Info);
verify_policy({exactly, N}, Info) ->
    (N - 1) == length(proplists:get_value(slave_pids, Info));
verify_policy({nodes, Nodes}, Info) ->
    Master = node(proplists:get_value(pid, Info)),
    Slaves = [node(P) || P <- proplists:get_value(slave_pids, Info)],
    lists:sort(Nodes) == lists:sort([Master | Slaves]).

%% Policies
apply_policy(Cfg, undefined) ->
    _ = clear_policy(Cfg, ?POLICY);
apply_policy(Cfg, all) ->
    set_ha_policy(Cfg, ?POLICY, <<"all">>,
                  [{<<"ha-sync-mode">>, <<"automatic">>}]);
apply_policy(Cfg, {nodes, Nodes}) ->
    NNodes = [a2b(Node) || Node <- Nodes],
    set_ha_policy(Cfg, ?POLICY, {<<"nodes">>, NNodes},
                  [{<<"ha-sync-mode">>, <<"automatic">>}]);
apply_policy(Cfg, {exactly, Exactly}) ->
    set_ha_policy(Cfg, ?POLICY, {<<"exactly">>, Exactly},
                  [{<<"ha-sync-mode">>, <<"automatic">>}]).

