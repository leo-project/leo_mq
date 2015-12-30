%%====================================================================
%%
%% Leo MQ
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%% Leo MQ - TEST
%% @author yosuke hara
%% @doc
%% @end
%%====================================================================
-module(leo_mq_api_tests).

-include("leo_mq.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(QUEUE_ID_PUBLISHER, 'replicate_miss_queue').
-define(QUEUE_ID_CONSUMER,  'replicate_miss_queue_consumer_1').

-define(TEST_KEY_1, "air/on/g/string_1").
-define(TEST_KEY_2, "air/on/g/string_2").
-define(TEST_KEY_3, "air/on/g/string_3").
-define(TEST_KEY_4, "air/on/g/string_4").
-define(TEST_KEY_5, "air/on/g/string_5").
-define(TEST_META_1, [{key,       ?TEST_KEY_1},
                      {vnode_id,  1},
                      {clock,     9},
                      {timestamp, 8},
                      {checksum,  7}]).
-define(TEST_META_2, [{key,       ?TEST_KEY_2},
                      {vnode_id,  2},
                      {clock,     19},
                      {timestamp, 18},
                      {checksum,  17}]).
-define(TEST_META_3, [{key,       ?TEST_KEY_3},
                      {vnode_id,  3},
                      {clock,     29},
                      {timestamp, 28},
                      {checksum,  27}]).
-define(TEST_META_4, [{key,       ?TEST_KEY_4},
                      {vnode_id,  4},
                      {clock,     39},
                      {timestamp, 38},
                      {checksum,  37}]).
-define(TEST_META_5, [{key,       ?TEST_KEY_5},
                      {vnode_id,  5},
                      {clock,     49},
                      {timestamp, 48},
                      {checksum,  47}]).

-define(TEST_CLIENT_MOD, 'mq_test_client').

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

publish_test_() ->
    {setup,
     fun ( ) ->
             application:start(leo_mq)
     end,
     fun (_) ->
             application:stop(leo_mq),
             ok
     end,
     [
      {"test pub/sub-2",
       {timeout, timer:seconds(1000),fun publish/0}}
     ]}.


publish() ->
    S = os:cmd("pwd"),
    Path = string:substr(S, 1, length(S) -1) ++ "/queue",
    os:cmd("rm -rf " ++ Path),

    meck:new(?TEST_CLIENT_MOD, [non_strict]),
    meck:expect(?TEST_CLIENT_MOD, handle_call,
                fun({consume, Id, MsgBin}) ->
                        ?debugVal({consume, Id, binary_to_term(MsgBin)}),
                        case binary_to_term(MsgBin) of
                            ?TEST_META_1 -> ok;
                            ?TEST_META_2 -> ok;
                            ?TEST_META_3 -> ok;
                            ?TEST_META_4 -> ok;
                            ?TEST_META_5 -> ok;
                            _ ->
                                throw({error, invalid_message})
                        end;
                   ({publish, Id, Reply}) ->
                        ?debugVal({publish, Id, Reply}),
                        ok
                end),
    Ret =  leo_mq_api:new(?QUEUE_ID_PUBLISHER, [{module, ?TEST_CLIENT_MOD},
                                                {root_path, Path},
                                                {db_procs, 8},
                                                {num_of_batch_processes, 10},
                                                {max_interval, 1000},
                                                {min_interval, 100}]),
    ?assertEqual(ok, Ret),
    ok = leo_mq_api:publish(
           ?QUEUE_ID_PUBLISHER, list_to_binary(?TEST_KEY_1), term_to_binary(?TEST_META_1)),
    ok = leo_mq_api:publish(
           ?QUEUE_ID_PUBLISHER, list_to_binary(?TEST_KEY_2), term_to_binary(?TEST_META_2)),
    ok = leo_mq_api:publish(
           ?QUEUE_ID_PUBLISHER, list_to_binary(?TEST_KEY_3), term_to_binary(?TEST_META_3)),
    ok = leo_mq_api:publish(
           ?QUEUE_ID_PUBLISHER, list_to_binary(?TEST_KEY_4), term_to_binary(?TEST_META_4)),
    ok = leo_mq_api:publish(
           ?QUEUE_ID_PUBLISHER, list_to_binary(?TEST_KEY_5), term_to_binary(?TEST_META_5)),

    timer:sleep(500),
    ok = check_state(),

    {ok, Stats} = leo_mq_api:status(?QUEUE_ID_PUBLISHER),
    ?debugVal(Stats),
    Count = leo_misc:get_value(?MQ_CNS_PROP_NUM_OF_MSGS, Stats),
    ?assertEqual(0, Count),

    meck:unload(),
    os:cmd("rm -rf " ++ Path),
    ok.


check_state() ->
    check_state_1(0).
check_state_1(3) ->
    ok;
check_state_1(Times) ->
    timer:sleep(500),
    case leo_mq_consumer:state(?QUEUE_ID_CONSUMER) of
        {ok, ?ST_IDLING} ->
            check_state_1(Times + 1);
        {ok,_Other} ->
            check_state_1(Times)
    end.


pub_sub_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### MQ.START ###"),
             ok
     end,
     fun (_) ->
             ?debugVal("### MQ.END ###"),
             ok
     end,
     [
      {"test pub/sub-1",
       {timeout, timer:seconds(120),fun pub_sub_1/0}},
      {"test pub/sub-2",
       {timeout, timer:seconds(120),fun pub_sub_2/0}}
     ]}.

pub_sub_1() ->
    %% prepare
    application:start(leo_mq),
    Path = queue_db_path(),
    os:cmd("rm -rf " ++ Path),

    meck:new(?TEST_CLIENT_MOD, [non_strict]),
    meck:expect(?TEST_CLIENT_MOD, handle_call,
                fun({consume,_Id,_MsgBin}) ->
                        ok;
                   ({publish,_Id,_Reply}) ->
                        ok
                end),
    Path = queue_db_path(),
    Ret  = leo_mq_api:new(?QUEUE_ID_PUBLISHER, [{module, ?TEST_CLIENT_MOD},
                                                {root_path, Path},
                                                {db_procs, 8},
                                                {regularx_batch_of_msgs, 100},
                                                {max_interval, 1000},
                                                {min_interval, 100}]),
    ?assertEqual(ok, Ret),

    %% publish messages
    ok = publish_messages(100),
    {ok, Stats} = leo_mq_api:status(?QUEUE_ID_PUBLISHER),
    ?debugVal(Stats),

    TotalMsgs_1 = leo_misc:get_value(?MQ_CNS_PROP_NUM_OF_MSGS, Stats),
    ?assertEqual(true, TotalMsgs_1 > 0),

    %% suspend the message consumption
    timer:sleep(timer:seconds(1)),
    ?debugVal("*** SUSPEND ***"),
    ok = leo_mq_api:suspend(?QUEUE_ID_PUBLISHER),

    {ok, State_1} = leo_mq_consumer:state(?QUEUE_ID_CONSUMER),
    ?assertEqual(?ST_SUSPENDING, State_1),

    {ok, [Consumers_1|_]} = leo_mq_api:consumers(),
    ?debugVal(Consumers_1),
    ?assertEqual(?ST_SUSPENDING, leo_misc:get_value(?MQ_CNS_PROP_STATUS, Consumers_1#mq_state.state)),

    %% resume the message consumption
    timer:sleep(timer:seconds(1)),
    ?debugVal("*** RESUME ***"),
    ok = leo_mq_api:resume(?QUEUE_ID_PUBLISHER),

    %% check incr/decr interval
    [ok = leo_mq_api:decrease(?QUEUE_ID_PUBLISHER) || _N <- lists:seq(1, 12)],
    timer:sleep(timer:seconds(5)),
    {ok, State_2} = leo_mq_consumer:state(?QUEUE_ID_CONSUMER),
    ?assertEqual(?ST_SUSPENDING, State_2),

    %% check current status
    timer:sleep(timer:seconds(10)),
    [ok = leo_mq_api:increase(?QUEUE_ID_PUBLISHER) || _N <- lists:seq(1, 12)],

    ok = check_state(),
    {ok, State_3} = leo_mq_consumer:state(?QUEUE_ID_CONSUMER),
    ?debugVal(State_3),
    ?assertEqual(?ST_IDLING, State_3),

    {ok, Stats_1} = leo_mq_api:status(?QUEUE_ID_PUBLISHER),
    ?debugVal(Stats_1),
    TotalMsgs_2 = leo_misc:get_value(?MQ_CNS_PROP_NUM_OF_MSGS, Stats_1),
    ?assertEqual(0, TotalMsgs_2),

    %% retrieve registered consumers
    timer:sleep(timer:seconds(3)),
    {ok, [Consumers_2|_]} = leo_mq_api:consumers(),
    ?debugVal(Consumers_2),
    ?assertEqual(?ST_IDLING, leo_misc:get_value(?MQ_CNS_PROP_STATUS, Consumers_2#mq_state.state)),

    %% execute after processing
    meck:unload(),
    application:stop(leo_mq),
    ok.

pub_sub_2() ->
    ?debugVal("*** TEST - pub/sub#2 ***"),
    %% prepare
    application:start(leo_mq),
    Path = queue_db_path(),
    os:cmd("rm -rf " ++ Path),

    meck:new(?TEST_CLIENT_MOD, [non_strict]),
    meck:expect(?TEST_CLIENT_MOD, handle_call,
                fun({consume,_Id,_MsgBin}) ->
                        ?debugVal(_Id),
                        timer:sleep(timer:seconds(30)),
                        ok;
                   ({publish,_Id,_Reply}) ->
                        ok
                end),
    Path = queue_db_path(),
    Ret  = leo_mq_api:new(?QUEUE_ID_PUBLISHER, [{module, ?TEST_CLIENT_MOD},
                                                {root_path, Path},
                                                {db_procs, 8},
                                                {regularx_batch_of_msgs, 10},
                                                {max_interval, 1000},
                                                {min_interval, 100}]),
    ?assertEqual(ok, Ret),

    %% publish messages
    ok = publish_messages(1),

    timer:sleep(timer:seconds(30)),
    ok = check_state(),
    {ok, Stats} = leo_mq_api:status(?QUEUE_ID_PUBLISHER),
    ?debugVal(Stats),

    %% execute after processing
    meck:unload(),
    application:stop(leo_mq),
    ok.


%% @private
queue_db_path() ->
    S = os:cmd("pwd"),
    Path = string:substr(S, 1, length(S) -1) ++ "/queue",
    Path.

%% @private
publish_messages(0) ->
    ok;
publish_messages(Index) ->
    Key = list_to_binary(lists:append(["air/on/g/string_", integer_to_list(Index)])),
    Msg = term_to_binary([{key,   Key},
                          {index, Index},
                          {clock, leo_date:clock()}]),
    ok = leo_mq_api:publish(?QUEUE_ID_PUBLISHER, Key, Msg),
    publish_messages(Index - 1).

-endif.
