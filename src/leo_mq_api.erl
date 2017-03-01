%%======================================================================
%%
%% Leo MQ
%%
%% Copyright (c) 2012-2017 Rakuten, Inc.
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

-include("leo_mq.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([new/2, new/3,
         publish/3, suspend/1, resume/1,
         status/1,
         consumers/0,
         increase/1, decrease/1,
         update_consumer_stats/4
        ]).

-define(APP_NAME, 'leo_mq').
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
    ok = leo_misc:set_env(leo_mq, {props, Id}, Props_1),
    ok = start_child_1(RefSup, Props_1),
    ok.


%% @private
-spec(prop_list_to_mq_properties(Id, Mod, Props) ->
             #mq_properties{} when Id::atom(),
                                   Mod::module(),
                                   Props::[tuple()]).
prop_list_to_mq_properties(Id, Mod, Props) ->
    DBProcs = leo_misc:get_value(?MQ_PROP_DB_PROCS, Props, ?DEF_BACKEND_DB_PROCS),
    DBProcs_1 = case (DBProcs < ?DEF_BACKEND_DB_PROCS) of
                    true ->
                        ?DEF_BACKEND_DB_PROCS;
                    false ->
                        DBProcs
                end,
    Props_1 = #mq_properties{
                 publisher_id = Id,
                 mod_callback = leo_misc:get_value(?MQ_PROP_MOD, Props, Mod),
                 db_name = leo_misc:get_value(?MQ_PROP_DB_NAME, Props, ?DEF_BACKEND_DB),
                 db_procs = DBProcs_1,
                 cns_procs_per_db = leo_misc:get_value(?MQ_PROP_CNS_PROCS_PER_DB, Props, ?DEF_PROP_CNS_PROCS_PER_DB),
                 root_path = leo_misc:get_value(?MQ_PROP_ROOT_PATH, Props, ?DEF_DB_ROOT_PATH),
                 %% interval between batchs
                 max_interval = leo_misc:get_value(?MQ_PROP_INTERVAL_MAX, Props, ?DEF_CONSUME_MAX_INTERVAL),
                 regular_interval = leo_misc:get_value(?MQ_PROP_INTERVAL_REG, Props, ?DEF_CONSUME_REG_INTERVAL),
                 %% batch of messages
                 max_batch_of_msgs = leo_misc:get_value(?MQ_PROP_BATCH_MSGS_MAX, Props, ?DEF_CONSUME_MAX_BATCH_MSGS),
                 regular_batch_of_msgs = leo_misc:get_value(?MQ_PROP_BATCH_MSGS_REG, Props, ?DEF_CONSUME_REG_BATCH_MSGS),
                 %% num of steps
                 num_of_steps = leo_misc:get_value(?MQ_PROP_NUM_OF_STEPS, Props, ?DEF_CONSUME_NUM_OF_STEPS)},
    {MQDBMessageId,
     MQDBMessagePath} = ?backend_db_info(
                           Id, Props_1#mq_properties.root_path),
    Props_2 = Props_1#mq_properties{
                mqdb_id = MQDBMessageId,
                mqdb_path = MQDBMessagePath},
    Props_2.


%% @doc Publish a message into the queue
%%
-spec(publish(Id, KeyBin, MessageBin) ->
             ok | {error, any()} when Id::atom(),
                                      KeyBin::binary(),
                                      MessageBin::binary()).
publish(Id, KeyBin, MessageBin) ->
    case leo_misc:get_env(leo_mq, {props, Id}) of
        undefined ->
            {error, not_initialized};
        {ok, #mq_properties{db_procs = _DB_Procs}} ->
            PubId = ?publisher_id(Id,_DB_Procs, KeyBin),
            leo_mq_server:enqueue(PubId, KeyBin, MessageBin)
    end.


%% @doc Suspend consumption of messages in the queue
%%
-spec(suspend(Id) ->
             ok | {error, any()} when Id::atom()).
suspend(Id) ->
    exec_sub_fun(Id, fun suspend/3).
suspend(Id, SeqNo, SubNo) ->
    [leo_mq_consumer:suspend(CnsId) ||
        CnsId <- gen_consumer_id(Id, SeqNo, SubNo, [])],
    ok.


%% @doc Resume consumption of messages in the queue
%%
-spec(resume(Id) ->
             ok | {error, any()} when Id::atom()).
resume(Id) ->
    exec_sub_fun(Id, fun resume/3).
resume(Id, SeqNo, SubNo) ->
    [leo_mq_consumer:resume(CnsId) ||
        CnsId  <- gen_consumer_id(Id, SeqNo, SubNo, [])],
    ok.


%% @doc Retrieve a current state from the queue
%%
-spec(status(Id) ->
             {ok, [{atom(), any()}]} when Id::atom()).
status(Id) ->
    Ret = case leo_backend_db_api:status(?backend_db_info(Id)) of
              not_found ->
                  0;
              DBStat ->
                  lists:foldl(fun([{key_count, Count}], Acc) ->
                                      Acc + Count
                              end, 0, DBStat)
          end,
    {State_2, BatchOfMsgs_1, Interval_1} =
        case leo_misc:get_env(leo_mq, {state, Id}) of
            undefined ->
                {?ST_IDLING, 0, 0};
            {ok, {State, BatchOfMsgs, Interval}} ->
                State_1 = case (Ret < 1) of
                              true ->
                                  ?ST_IDLING;
                              false ->
                                  State
                          end,
                {State_1, BatchOfMsgs, Interval}
        end,
    {ok, [{?MQ_CNS_PROP_NUM_OF_MSGS, Ret},
          {?MQ_CNS_PROP_STATUS, State_2},
          {?MQ_CNS_PROP_BATCH_OF_MSGS, BatchOfMsgs_1},
          {?MQ_CNS_PROP_INTERVAL, Interval_1}
         ]}.
    %% end.


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
            Sets = sets:from_list([ #mq_state{id = ?publisher_id(Worker)} ||
                                      {Worker,_,worker,[leo_mq_consumer]} <- Children ]),
            Consumers = sets:to_list(Sets),
            Consumers_1 = lists:map(fun(Id) ->
                                            {ok, State} = status(Id),
                                            #mq_state{id = Id, state = State}
                                    end, consumers_1(Consumers, sets:new())),
            {ok, Consumers_1}
    end.

%% @private
consumers_1([], SoFar) ->
    sets:to_list(SoFar);
consumers_1([#mq_state{id = Id}|Rest], SoFar) ->
    consumers_1(Rest, sets:add_element(Id, SoFar)).


%% @doc Increase the comsumption processing
%%
-spec(increase(Id) ->
             ok | {error, any()} when Id::atom()).
increase(Id) ->
    exec_sub_fun(Id, fun increase/3).

%% @private
increase(Id, SeqNo, SubNo) ->
    [leo_mq_consumer:increase(CnsId) ||
        CnsId  <- gen_consumer_id(Id, SeqNo, SubNo, [])],
    ok.


%% @doc Decrease the comsumption processing
%%
-spec(decrease(Id) ->
             ok | {error, any()} when Id::atom()).
decrease(Id) ->
    exec_sub_fun(Id, fun decrease/3).

%% @private
decrease(Id, SeqNo, SubNo) ->
    [leo_mq_consumer:decrease(CnsId) ||
        CnsId  <- gen_consumer_id(Id, SeqNo, SubNo, [])],
    ok.

%% @doc Update consumer status
-spec(update_consumer_stats(PubId, State, BatchOfMsgs, Interval) ->
             ok when PubId::atom(),
                     State::state_of_mq(),
                     BatchOfMsgs::non_neg_integer(),
                     Interval::non_neg_integer()).
update_consumer_stats(PubId, State, BatchOfMsgs, Interval) ->
    State_1 = case leo_backend_db_api:status(
                     ?backend_db_info(PubId)) of
                  not_found ->
                      ?ST_IDLING;
                  DBStat ->
                      case (lists:foldl(fun([{key_count, Count}], Acc) ->
                                                Acc + Count
                                        end, 0, DBStat) > 0) of
                          true ->
                              State;
                          false ->
                              ?ST_IDLING
                      end
              end,
    leo_misc:set_env(leo_mq, {state, PubId},
                     {State_1, BatchOfMsgs, Interval}).


%%--------------------------------------------------------------------
%% INNTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Start 'leo_mq_server'
%% @private
start_child_1(RefSup, #mq_properties{db_procs = DbProcs} = Props) ->
    start_child_2(RefSup, Props, DbProcs).

%% @doc Start 'leo_mq_consumer'
%% @private
start_child_2(_,_,0) ->
    ok;
start_child_2(RefSup, #mq_properties{publisher_id = PubId,
                                     cns_procs_per_db = CnsProcsPerDB} = Props, WorkerSeqNum) ->
    PubId_1 = ?publisher_id(PubId, WorkerSeqNum),
    case supervisor:start_child(
           RefSup, {PubId_1,
                    {leo_mq_server, start_link, [PubId_1, WorkerSeqNum, Props]},
                    permanent, 2000, worker, [leo_mq_server]}) of
        {ok,_Pid} ->
            case start_child_3(RefSup, Props, WorkerSeqNum, CnsProcsPerDB) of
                ok ->
                    start_child_2(RefSup, Props, WorkerSeqNum - 1)
            end;
        {error, Reason} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "start_child_2/3"},
                                    {line, ?LINE}, {body, Reason}]),
            case leo_mq_sup:stop() of
                ok ->
                    exit(invalid_launch);
                not_started ->
                    exit(noproc)
            end
    end.

%% @private
start_child_3(_RefSup,_Props,_WorkerSeqNum, 0) ->
    ok;
start_child_3(RefSup, #mq_properties{publisher_id = PubId} = Props,
              WorkerSeqNum, CnsProcsPerDB) ->
    ConsumerId = ?consumer_id(PubId, WorkerSeqNum, CnsProcsPerDB),

    case supervisor:start_child(
           RefSup, {ConsumerId,
                    {leo_mq_consumer, start_link,
                     [ConsumerId, PubId, Props, WorkerSeqNum]},
                    permanent, 2000, worker, [leo_mq_consumer]}) of
        {ok, _Pid} ->
            start_child_3(RefSup, Props, WorkerSeqNum, (CnsProcsPerDB - 1));
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "start_child_3/4"},
                                    {line, ?LINE}, {body, Cause}]),
            case leo_mq_sup:stop() of
                ok ->
                    exit(launch_failed);
                not_started ->
                    exit(noproc)
            end
    end.


%% @private
exec_sub_fun(Id, Fun) ->
    case leo_misc:get_env(leo_mq, {props, Id}) of
        {ok, #mq_properties{db_procs = NumOfDbProcs,
                            cns_procs_per_db = CnsProcsPerDb}} ->
            Fun(Id, NumOfDbProcs, CnsProcsPerDb);
        _ ->
            {error, not_initialized}
    end.


%% @doc Generate consumer-id's list
%% @privare
gen_consumer_id(_, 0,_, Acc) ->
    lists:reverse(Acc);
gen_consumer_id(Id, SeqNo, SubNo, Acc) ->
    Acc_1 = gen_consumer_id_1(Id, SeqNo, SubNo, []),
    gen_consumer_id(Id, (SeqNo - 1), SubNo, Acc ++ Acc_1).

%% @private
gen_consumer_id_1(_,_,0, Acc) ->
    lists:reverse(Acc);
gen_consumer_id_1(Id, SeqNo, SubNo, Acc) ->
    Acc_1 = [?consumer_id(Id, SeqNo, SubNo)|Acc],
    gen_consumer_id_1(Id, SeqNo, (SubNo - 1), Acc_1).
