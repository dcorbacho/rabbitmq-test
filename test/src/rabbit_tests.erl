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
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%
-module(rabbit_tests).

-compile([export_all]).

-export([all_tests/0]).

-import(rabbit_misc, [pget/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% While this test uses only a single node, it's started using
%%% the multi-node test framework. The reason for this is that resource
%%% alarm is being set as a part of the test, and stopping a node is
%%% easier than bringing the standalone-tests node into a healthy state.
%%% Initialization is done in a group functions so they later can be
%%% moved to rabbit_test_configs if needed.
disconnect_detected_during_alarm_with() ->
    [fun(Cfg) -> rabbit_test_configs:start_nodes(Cfg, [a]) end
    , fun([ACfg]) ->
              rabbit_test_configs:rabbitmqctl(ACfg, "set_vm_memory_high_watermark 0.000000001"),
              [ACfg]
      end
    , fun([ACfg]) ->
              Port = pget(port, ACfg),
              Heartbeat = 1,
              {ok, Conn} = amqp_connection:start(#amqp_params_network{port = Port,
                                                                      heartbeat = Heartbeat}),
              {ok, Channel} = amqp_connection:open_channel(Conn),
              [[{heartbeat, Heartbeat}, {connection, Conn}, {channel, Channel} | ACfg]]
      end
    ].

disconnect_detected_during_alarm([ACfg]) ->
    Conn = pget(connection, ACfg),
    amqp_connection:register_blocked_handler(Conn, self()),
    Ch = pget(channel, ACfg),
    Publish = #'basic.publish'{routing_key = <<"nowhere-to-go">>},
    amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"foobar">>}),
    receive
        % Check that connection was indeed blocked
        #'connection.blocked'{} -> ok
    after
        1000 -> exit(connection_was_not_blocked)
    end,

    %% Connection is blocked, now we should forcefully kill it
    {'EXIT', _} = (catch amqp_connection:close(Conn, 10)),

    ListConnections =
        fun() ->
                rpc:call(pget(node, ACfg), rabbit_networking, connection_info_all, [])
        end,

    %% We've already disconnected, but blocked connection still should still linger on.
    [SingleConn] = ListConnections(),
    blocked = pget(state, SingleConn),

    %% It should definitely go away after 2 heartbeat intervals.
    timer:sleep(round(2.5 * 1000 * pget(heartbeat, ACfg))),
    [] = ListConnections(),

    passed.

rabbitmqctl_list_consumers(Config) ->
    StdOut = rabbit_test_configs:rabbitmqctl(Config, "list_consumers"),
    [<<"Listing consumers", _/binary>> | ConsumerRows] = re:split(StdOut, <<"\n">>, [trim]),
    CTags = [ lists:nth(3, re:split(Row, <<"\t">>)) || Row <- ConsumerRows ],
    CTags.

list_consumers_sanity_check_with() ->
    start_and_connect_a.

list_consumers_sanity_check([ACfg]) ->
    Chan = pget(channel, ACfg),
    %% this queue is not cleaned up because the entire node is
    %% reset between tests
    QName = <<"list_consumers_q">>,
    #'queue.declare_ok'{} = amqp_channel:call(Chan, #'queue.declare'{queue = QName}),

    %% No consumers even if we have some queues
    ?assertEqual([], rabbitmqctl_list_consumers(ACfg)),

    %% Several consumers on single channel should be correctly reported
    #'basic.consume_ok'{consumer_tag = CTag1} = amqp_channel:call(Chan, #'basic.consume'{queue = QName}),
    #'basic.consume_ok'{consumer_tag = CTag2} = amqp_channel:call(Chan, #'basic.consume'{queue = QName}),
    ?assertEqual(lists:sort([CTag1, CTag2]),
                 lists:sort(rabbitmqctl_list_consumers(ACfg))),

    %% `rabbitmqctl report` shares some code with `list_consumers`, so check that it also reports both channels
    ReportStdOut = rabbit_test_configs:rabbitmqctl(ACfg, "list_consumers"),
    ReportLines = re:split(ReportStdOut, <<"\n">>, [trim]),
    ReportCTags = [lists:nth(3, re:split(Row, <<"\t">>)) || <<"list_consumers_q", _/binary>> = Row <- ReportLines],
    ?assertEqual(lists:sort([CTag1, CTag2]),
                 lists:sort(ReportCTags)).

list_queues_online_and_offline_with() ->
    cluster_ab.

list_queues_online_and_offline([ACfg, BCfg]) ->
    ACh = pget(channel, ACfg),
    %% Node B will be stopped
    BCh = pget(channel, BCfg),
    #'queue.declare_ok'{} = amqp_channel:call(ACh, #'queue.declare'{queue = <<"q_a_1">>, durable = true}),
    #'queue.declare_ok'{} = amqp_channel:call(ACh, #'queue.declare'{queue = <<"q_a_2">>, durable = true}),
    #'queue.declare_ok'{} = amqp_channel:call(BCh, #'queue.declare'{queue = <<"q_b_1">>, durable = true}),
    #'queue.declare_ok'{} = amqp_channel:call(BCh, #'queue.declare'{queue = <<"q_b_2">>, durable = true}),

    rabbit_test_configs:rabbitmqctl(BCfg, "stop"),

    GotUp = lists:sort(rabbit_test_configs:rabbitmqctl_list(ACfg, "list_queues --online name")),
    ExpectUp = [[<<"q_a_1">>], [<<"q_a_2">>]],
    ?assertEqual(ExpectUp, GotUp),

    GotDown = lists:sort(rabbit_test_configs:rabbitmqctl_list(ACfg, "list_queues --offline name")),
    ExpectDown = [[<<"q_b_1">>], [<<"q_b_2">>]],
    ?assertEqual(ExpectDown, GotDown),

    GotAll = lists:sort(rabbit_test_configs:rabbitmqctl_list(ACfg, "list_queues name")),
    ExpectAll = ExpectUp ++ ExpectDown,
    ?assertEqual(ExpectAll, GotAll),

    ok.
