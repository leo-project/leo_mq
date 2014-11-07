%%====================================================================
%%
%% Leo MQ
%%
%% Copyright (c) 2012-2014 Rakuten, Inc.
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
-author('yosuke hara').

-include("leo_mq.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(QUEUE_ID_PUBLISHER, 'replicate_miss_queue').
-define(QUEUE_ID_CONSUMER,  'replicate_miss_queue_consumer').

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

mq_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun publish_/1
                          ]]}.

setup() ->
    application:start(leo_mq),

    S = os:cmd("pwd"),
    Path = string:substr(S, 1, length(S) -1) ++ "/queue",
    os:cmd("rm -rf " ++ Path),
    Path.


teardown(Path) ->
    meck:unload(),
    os:cmd("rm -rf " ++ Path),
    application:stop(leo_mq),
    ok.


publish_(Path) ->
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
                                                     {num_of_batch_processes, 10},
                                                     {max_interval, 500},
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

    timer:sleep(1000),
    ok = check_state(),

    {ok, Count} = leo_mq_api:status(?QUEUE_ID_PUBLISHER),
    ?debugVal(Count),
    ?assertEqual(0, Count),
    ok.


check_state() ->
    timer:sleep(10),
    case leo_mq_consumer:state(?QUEUE_ID_CONSUMER) of
        {ok, ?ST_IDLING} ->
            ok;
        {ok,_Other} ->
            check_state()
    end.



%% =========================================================
%%
%% =========================================================
pub_sub_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### MQ.START ###"),
             application:start(leo_mq),
             Path = queue_db_path(),
             os:cmd("rm -rf " ++ Path),
             ok
     end,
     fun (_) ->
             meck:unload(),
             application:stop(leo_mq),
             ?debugVal("### MQ.END ###"),
             ok
     end,
     [
      {"test pub/sub",
       {timeout, timer:seconds(120),fun pub_sub/0}}
     ]}.


pub_sub() ->
    meck:new(?TEST_CLIENT_MOD, [non_strict]),
    meck:expect(?TEST_CLIENT_MOD, handle_call,
                fun({consume,_Id,_MsgBin}) ->
                        ok;
                   ({publish,_Id,_Reply}) ->
                        ok
                end),
    Path = queue_db_path(),
    Ret  =  leo_mq_api:new(?QUEUE_ID_PUBLISHER, [{module, ?TEST_CLIENT_MOD},
                                                 {root_path, Path},
                                                 {num_of_batch_processes, 10},
                                                 {max_interval, 500},
                                                 {min_interval, 100}]),
    ?assertEqual(ok, Ret),

    %% publish messages
    ok = publish_messages(100),
    timer:sleep(timer:seconds(1)),

    %% suspend the message consumption
    ?debugVal("*** SUSPEND ***"),
    ok = leo_mq_api:suspend(?QUEUE_ID_PUBLISHER),

    {ok, TotalMsgs_1} = leo_mq_api:status(?QUEUE_ID_PUBLISHER),
    ?debugVal(TotalMsgs_1),
    ?assertEqual(true, TotalMsgs_1 > 0),

    {ok, State_1} = leo_mq_consumer:state(?QUEUE_ID_CONSUMER),
    ?assertEqual(?ST_SUSPENDING, State_1),

    {ok, Consumers_1} = leo_mq_api:consumers(),
    ?debugVal(Consumers_1),
    ?assertEqual(true, length([SuspendingProc
                               || #mq_state{id = SuspendingProc,
                                            state = ?ST_SUSPENDING}
                                      <- Consumers_1]) > 0),

    %% resume the message consumption
    timer:sleep(timer:seconds(5)),
    ?debugVal("*** RESUME ***"),
    ok = leo_mq_api:resume(?QUEUE_ID_PUBLISHER),

    %% check current status
    ok = check_state(),
    {ok, State_2} = leo_mq_consumer:state(?QUEUE_ID_CONSUMER),
    ?assertEqual(?ST_IDLING, State_2),

    {ok, TotalMsgs_2} = leo_mq_api:status(?QUEUE_ID_PUBLISHER),
    ?assertEqual(0, TotalMsgs_2),

    %% retrieve registered consumers
    {ok, Consumers_2} = leo_mq_api:consumers(),
    ?debugVal(Consumers_2),
    ?assertEqual(true, length([IdlingProc
                               || #mq_state{id = IdlingProc,
                                            state = ?ST_IDLING}
                                      <- Consumers_2]) > 0),
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
