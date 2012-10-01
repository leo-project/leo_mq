%%====================================================================
%%
%% Leo MQ
%%
%% Copyright (c) 2012
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

-define(QUEUE_ID_REPLICATE_MISS, 'replicate_miss_queue').

-define(TEST_KEY_1, "air/on/g/string_1").
-define(TEST_META_1, [{key,       ?TEST_KEY_1},
                      {vnode_id,  1},
                      {clock,     9},
                      {timestamp, 8},
                      {checksum,  7}]).

-define(TEST_CLIENT_MOD, 'mq_test_client').
%% -define(TEST_DB_PATH,    "mq-test").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

mq_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun publish_/1
                          ]]}.

setup() ->
    meck:new(leo_util),
    meck:expect(leo_util, clock, fun() -> 1000 end),
    meck:expect(leo_util, now,   fun() -> 1000 end),

    S = os:cmd("pwd"),
    Path = string:substr(S, 1, length(S) -1) ++ "/queue",
    os:cmd("rm -rf " ++ Path),
    Path.


teardown(Path) ->
    meck:unload(),
    os:cmd("rm -rf " ++ Path),

    leo_mq_sup:stop(),
    application:stop(leo_backend_db),
    application:stop(leo_mq),
    ok.


publish_(Path) ->
    Ret =  leo_mq_api:new(?QUEUE_ID_REPLICATE_MISS, [{module,   ?TEST_CLIENT_MOD},
                                                      {root_path, Path},
                                                      {max_interval, 500},
                                                      {min_interval, 100}]),
    ?assertEqual(ok, Ret),

    meck:new(?TEST_CLIENT_MOD),
    meck:expect(?TEST_CLIENT_MOD, handle_call,
                fun(consume, Id, MsgBin) ->
                        ?debugVal({consume, Id, binary_to_term(MsgBin)}),
                        ?assertEqual(?QUEUE_ID_REPLICATE_MISS, Id),
                        ?assertEqual(?TEST_META_1, binary_to_term(MsgBin)),
                        ok
                end),
    meck:expect(?TEST_CLIENT_MOD, handle_call,
                fun(publish, Reply) ->
                        ?debugVal({publish, Reply}),
                        ok
                end),

    ok = leo_mq_api:publish(
           ?QUEUE_ID_REPLICATE_MISS, list_to_binary(?TEST_KEY_1), term_to_binary(?TEST_META_1)),
    timer:sleep(500),

    {ok, {C0, C1}} = leo_mq_api:status(?QUEUE_ID_REPLICATE_MISS),
    ?debugVal({C0, C1}),
    ?assertEqual(0, C0),
    ?assertEqual(0, C1),
    ok.

-endif.
