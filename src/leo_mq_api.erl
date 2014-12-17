%%======================================================================
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
%% ---------------------------------------------------------------------
%% Leo MQ - API
%% @doc leo_mq's API
%% @reference https://github.com/leo-project/leo_mq/blob/master/src/leo_mq_api.erl
%% @end
%%======================================================================
-module(leo_mq_api).

-author('Yosuke Hara').

-include("leo_mq.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([new/2, new/3,
         publish/3, suspend/1, resume/1,
         status/1,
         consumers/0,
         incr_interval/1, decr_interval/1,
         incr_batch_of_msgs/1, decr_batch_of_msgs/1
        ]).

-define(APP_NAME,      'leo_mq').
-define(DEF_DB_MODULE, 'leo_mq_eleveldb'). % Not used in anywhere.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Create a new message-queue server
%%
-spec(new(Id, Props) ->
             ok when Id::atom(),
                     Props::[atom()]|#mq_properties{}).
new(Id, Props) when is_list(Props) ->
    case leo_misc:get_value(?MQ_PROP_MOD, Props, undefined) of
        undefined ->
            {error, badarg};
        Mod ->
            new(Id, prop_list_to_mq_properties(Id, Mod, Props))
    end;
new(Id, Props) ->
    new(leo_mq_sup, Id, Props).

-spec(new(RefSup, Id, Props) ->
             ok  when RefSup::atom()|pid(),
                      Id::atom(),
                      Props::[atom()]|#mq_properties{}).
new(RefSup, Id, Props) ->
    Props_1 = case is_list(Props) of
                  true ->
                      prop_list_to_mq_properties(Id, undefined, Props);
                  false ->
                      Props
              end,

    ok = start_child_1(RefSup, Props_1),
    ok = start_child_2(RefSup, Props_1),
    ok.


%% @private
-spec(prop_list_to_mq_properties(Id, Mod, Props) ->
             #mq_properties{} when Id::atom(),
                                   Mod::module(),
                                   Props::[tuple()]).
prop_list_to_mq_properties(Id, Mod, Props) ->
    Props_1 = #mq_properties{
                 publisher_id = Id,
                 consumer_id  = ?consumer_id(Id),
                 mod_callback = leo_misc:get_value(?MQ_PROP_MOD,          Props, Mod),
                 db_name      = leo_misc:get_value(?MQ_PROP_DB_NAME,      Props, ?DEF_BACKEND_DB),
                 db_procs     = leo_misc:get_value(?MQ_PROP_DB_PROCS,     Props, ?DEF_BACKEND_DB_PROCS),
                 root_path    = leo_misc:get_value(?MQ_PROP_ROOT_PATH,    Props, ?DEF_DB_ROOT_PATH),
                 %% interval between batchs
                 max_interval     = leo_misc:get_value(?MQ_PROP_INTERVAL_MAX,  Props, ?DEF_CONSUME_MAX_INTERVAL),
                 min_interval     = leo_misc:get_value(?MQ_PROP_INTERVAL_MIN,  Props, ?DEF_CONSUME_MIN_INTERVAL),
                 regular_interval = leo_misc:get_value(?MQ_PROP_INTERVAL_REG,  Props, ?DEF_CONSUME_REG_INTERVAL),
                 step_interval    = leo_misc:get_value(?MQ_PROP_INTERVAL_STEP, Props, ?DEF_CONSUME_STEP_INTERVAL),
                 %% batch of messages
                 max_batch_of_msgs     = leo_misc:get_value(?MQ_PROP_BATCH_MSGS_MAX,  Props, ?DEF_CONSUME_MAX_BATCH_MSGS),
                 min_batch_of_msgs     = leo_misc:get_value(?MQ_PROP_BATCH_MSGS_MIN,  Props, ?DEF_CONSUME_MIN_BATCH_MSGS),
                 regular_batch_of_msgs = leo_misc:get_value(?MQ_PROP_BATCH_MSGS_REG,  Props, ?DEF_CONSUME_REG_BATCH_MSGS),
                 step_batch_of_msgs    = leo_misc:get_value(?MQ_PROP_BATCH_MSGS_STEP, Props, ?DEF_CONSUME_STEP_BATCH_MSGS)
                },
    {MQDBMessageId,
     MQDBMessagePath} = ?backend_db_info(Id,
                                         Props_1#mq_properties.root_path),
    Props_2 = Props_1#mq_properties{
                mqdb_id   = MQDBMessageId,
                mqdb_path = MQDBMessagePath
               },
    Props_2.


%% @doc Publish a message into the queue
%%
-spec(publish(Id, KeyBin, MessageBin) ->
             ok | {error, any()} when Id::atom(),
                                      KeyBin::binary(),
                                      MessageBin::binary()).
publish(Id, KeyBin, MessageBin) ->
    leo_mq_publisher:publish(Id, KeyBin, MessageBin).


%% @doc Suspend consumption of messages in the queue
%%
-spec(suspend(Id) ->
             ok | {error, any()} when Id::atom()).
suspend(Id) ->
    Id_1 = ?consumer_id(Id),
    leo_mq_consumer:suspend(Id_1).


%% @doc Resume consumption of messages in the queue
%%
-spec(resume(Id) ->
             ok | {error, any()} when Id::atom()).
resume(Id) ->
    Id_1 = ?consumer_id(Id),
    leo_mq_consumer:resume(Id_1).


%% @doc Retrieve a current state from the queue
%%
-spec(status(Id) ->
             {ok, [{atom(), any()}]} when Id::atom()).
status(Id) ->
    leo_mq_publisher:status(Id).


%% @doc Retrieve registered consumers
%%
-spec(consumers() ->
             {ok, Consumers} when Consumers::[{ConsumerId,
                                               State, MsgCount}],
                                  ConsumerId::atom(),
                                  State::state_of_mq(),
                                  MsgCount::non_neg_integer()).
consumers() ->
    case supervisor:which_children(leo_mq_sup) of
        [] ->
            {ok, []};
        Children ->
            {ok, [ #mq_state{id    = ?publisher_id(Worker),
                             state = element(2, leo_mq_publisher:status(?publisher_id(Worker)))
                            } || {Worker,_,worker,[leo_mq_consumer]} <- Children
                 ]}
    end.


%% @doc Increase waiting time
%%
-spec(incr_interval(Id) ->
             ok | {error, any()} when Id::atom()).
incr_interval(Id) ->
    Id_1 = ?consumer_id(Id),
    leo_mq_consumer:incr_interval(Id_1).


%% @doc Decrease waiting time
%%
-spec(decr_interval(Id) ->
             ok | {error, any()} when Id::atom()).
decr_interval(Id) ->
    Id_1 = ?consumer_id(Id),
    leo_mq_consumer:decr_interval(Id_1).


%% @doc Increase waiting time
%%
-spec(incr_batch_of_msgs(Id) ->
             ok | {error, any()} when Id::atom()).
incr_batch_of_msgs(Id) ->
    Id_1 = ?consumer_id(Id),
    leo_mq_consumer:incr_batch_of_msgs(Id_1).


%% @doc Decrease waiting time
%%
-spec(decr_batch_of_msgs(Id) ->
             ok | {error, any()} when Id::atom()).
decr_batch_of_msgs(Id) ->
    Id_1 = ?consumer_id(Id),
    leo_mq_consumer:decr_batch_of_msgs(Id_1).


%%--------------------------------------------------------------------
%% INNTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @private Start 'leo_mq_publisher'
start_child_1(RefSup, #mq_properties{publisher_id = PublisherId} = Props) ->
    case supervisor:start_child(
           RefSup, {PublisherId,
                    {leo_mq_publisher, start_link, [PublisherId, Props]},
                    permanent, 2000, worker, [leo_mq_publisher]}) of
        {ok, _Pid} ->
            ok;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "start_child_1/3"},
                                    {line, ?LINE}, {body, Cause}]),
            case leo_mq_sup:stop() of
                ok ->
                    exit(invalid_launch);
                not_started ->
                    exit(noproc)
            end
    end.

%% @private Start 'leo_mq_fsm_controller'
start_child_2(RefSup, #mq_properties{publisher_id = PublisherId,
                                     consumer_id  = ConsumerId} = Props) ->
    case supervisor:start_child(
           RefSup, {ConsumerId,
                    {leo_mq_consumer, start_link, [ConsumerId, PublisherId, Props]},
                    permanent, 2000, worker, [leo_mq_consumer]}) of
        {ok, _Pid} ->
            ok;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "start_child_2/3"},
                                    {line, ?LINE}, {body, Cause}]),
            case leo_mq_sup:stop() of
                ok ->
                    exit(launch_failed);
                not_started ->
                    exit(noproc)
            end
    end.
