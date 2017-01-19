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
%%======================================================================
-define(MQ_LOG_ID, 'queue').
-define(MQ_LOG_FILE_NAME, "queue").
-define(MQ_PROP_MOD, 'module').
-define(MQ_PROP_FUN, 'function').
-define(MQ_PROP_DB_NAME, 'db_name').
-define(MQ_PROP_DB_PROCS, 'db_procs').
-define(MQ_PROP_CNS_PROCS_PER_DB, 'cns_procs_per_db').
-define(MQ_PROP_ROOT_PATH, 'root_path').
-define(MQ_PROP_NUM_OF_BATCH_PROC, 'num_of_batch_processes').
-define(MQ_SUBSCRIBE_FUN, 'subscribe').

-define(MQ_PROP_INTERVAL_MAX, 'interval_max').
-define(MQ_PROP_INTERVAL_REG, 'interval_reg').
-define(MQ_PROP_BATCH_MSGS_MAX, 'batch_of_msgs_max').
-define(MQ_PROP_BATCH_MSGS_REG, 'batch_of_msgs_reg').
-define(MQ_PROP_NUM_OF_STEPS, 'num_of_steps').

-define(DEF_BACKEND_DB_PROCS, 3).
-define(DEF_PROP_CNS_PROCS_PER_DB, 1).
-define(DEF_BACKEND_DB, 'leveldb').
-define(DEF_DB_ROOT_PATH, "mq").

-ifdef(TEST).
-define(DEF_CONSUME_MAX_INTERVAL, 1000).
-define(DEF_CONSUME_MIN_INTERVAL, 10).
-define(DEF_CONSUME_REG_INTERVAL, 300).
-define(DEF_CONSUME_MAX_BATCH_MSGS, 10).
-define(DEF_CONSUME_MIN_BATCH_MSGS, 1).
-define(DEF_CONSUME_REG_BATCH_MSGS, 5).
-define(DEF_CONSUME_NUM_OF_STEPS, 5).
-else.
-define(DEF_CONSUME_MAX_INTERVAL, 300).
-define(DEF_CONSUME_MIN_INTERVAL, 0).
-define(DEF_CONSUME_REG_INTERVAL, 10).
-define(DEF_CONSUME_MAX_BATCH_MSGS, 30000).
-define(DEF_CONSUME_MIN_BATCH_MSGS,  1000).
-define(DEF_CONSUME_REG_BATCH_MSGS,  5000).
-define(DEF_CONSUME_NUM_OF_STEPS, 10).
-endif.

-define(MQ_CNS_PROP_NUM_OF_MSGS, 'consumer_num_of_msgs').
-define(MQ_CNS_PROP_STATUS, 'consumer_status').
-define(MQ_CNS_PROP_BATCH_OF_MSGS, 'consumer_batch_of_msgs').
-define(MQ_CNS_PROP_INTERVAL, 'consumer_interval').

-record(mq_properties, {
          publisher_id :: atom(),      %% publisher-id
          consumer_id :: atom(),       %% consumer-id
          mod_callback :: module(),    %% callback module name
          db_name :: atom(),           %% db's id
          db_procs = ?DEF_BACKEND_DB_PROCS :: pos_integer(), %% db's processes
          cns_procs_per_db = 1 :: pos_integer(),   %% consumer's processes/db
          root_path = [] :: string(),  %% db's path
          mqdb_id :: atom(),           %% mqdb's id
          mqdb_path = [] :: string(),  %% mqdb's path
          %% interval between batch-procs
          max_interval = ?DEF_CONSUME_MAX_INTERVAL :: pos_integer(),     %% max waiting time (default: 1000msec (1sec))
          regular_interval = ?DEF_CONSUME_REG_INTERVAL :: pos_integer(), %% regular waiting time (default: 300msec)
          %% num of batch procs
          max_batch_of_msgs = ?DEF_CONSUME_MAX_BATCH_MSGS :: pos_integer(),     %% max num of batch of messages
          regular_batch_of_msgs = ?DEF_CONSUME_REG_BATCH_MSGS :: pos_integer(), %% regular num of batch of messages
          %% num of steps
          num_of_steps = ?DEF_CONSUME_NUM_OF_STEPS :: pos_integer()
         }).

-record(mq_log, {
          type :: atom(),
          requested_at = 0 :: integer(),
          format = [] :: string(),
          message = [] :: string()}).


-define(ST_IDLING, 'idling').
-define(ST_RUNNING, 'running').
-define(ST_SUSPENDING, 'suspending').
-type(state_of_mq() :: ?ST_IDLING |
                       ?ST_RUNNING |
                       ?ST_SUSPENDING).

-record(mq_state, {
          id :: atom(),
          desc = [] :: string(),
          state :: [{atom(), any()}]
         }).


-define(EVENT_RUN, 'run').
-define(EVENT_DIAGNOSE, 'diagnose').
-define(EVENT_LOCK, 'lock').
-define(EVENT_SUSPEND, 'suspend').
-define(EVENT_RESUME, 'resume').
-define(EVENT_FINISH, 'finish').
-define(EVENT_STATE, 'state').
-define(EVENT_INCR, 'increase').
-define(EVENT_DECR, 'decrease').
-type(event_of_compaction() ::?EVENT_RUN |
                              ?EVENT_DIAGNOSE |
                              ?EVENT_LOCK |
                              ?EVENT_SUSPEND |
                              ?EVENT_RESUME |
                              ?EVENT_FINISH |
                              ?EVENT_STATE |
                              ?EVENT_INCR |
                              ?EVENT_DECR).

-define(DEF_CHECK_MAX_INTERVAL_1, timer:seconds(1)).
-define(DEF_CHECK_MIN_INTERVAL_1, timer:seconds(0)).
-define(DEF_CHECK_MAX_INTERVAL_2, timer:seconds(20)).
-define(DEF_CHECK_MIN_INTERVAL_2, timer:seconds(10)).

-define(DEF_CONSUMER_SUFFIX, "_consumer").


-define(consumer_id(_PubId,_SeqNo,_SubNo),
        list_to_atom(
          lists:append([atom_to_list(_PubId),
                        ?DEF_CONSUMER_SUFFIX,
                        "_", integer_to_list(_SeqNo),
                        "_", integer_to_list(_SubNo)
                       ]))).

-define(invoker_id(_PubId),
        list_to_atom(
          lists:append([atom_to_list(_PubId), ?DEF_INVOKER_SUFFIX]))).

-define(publisher_id(_ConsumerId),
        begin
            _StrId = atom_to_list(_ConsumerId),
            list_to_atom(
              string:substr(_StrId, 1,
                            string:str(_StrId, ?DEF_CONSUMER_SUFFIX) - 1))
        end).

-define(publisher_id(_PubId,_SeqNo),
        list_to_atom(
          lists:append([atom_to_list(_PubId),
                        "_", integer_to_list(_SeqNo)]))).

-define(publisher_id(_PubId,_DbProcs,_KeyBin),
        list_to_atom(
          lists:append([atom_to_list(_PubId),
                        "_",
                        integer_to_list((erlang:crc32(_KeyBin) rem _DbProcs) + 1)
                       ]))).


%% Retrieve the backend-db info
-define(DEF_DB_PATH_INDEX, "index").
-define(DEF_DB_PATH_MESSAGE, "message").

-define(backend_db_info(Id),
        list_to_atom(atom_to_list(Id) ++ "_message")).
-define(backend_db_info(Id, RootPath),
        begin
            MQDBMessageId = list_to_atom(atom_to_list(Id) ++ "_message"),
            NewRootPath = case (string:len(RootPath) == string:rstr(RootPath, "/")) of
                              true ->
                                  RootPath;
                              false ->
                                  RootPath ++ "/"
                          end,
            MQDBMessagePath = NewRootPath ++ ?DEF_DB_PATH_MESSAGE,
            {MQDBMessageId, MQDBMessagePath}
        end).

%% @doc Retrieve mq-consumption's step parameters
-define(step_comsumption_values(_MQProps),
        begin
            #mq_properties{regular_interval = _RegInterval,
                           regular_batch_of_msgs = _RegBatchMsgs,
                           num_of_steps = _NumOfSteps} = _MQProps,
            _StepBatchOfMsgs = leo_math:ceiling(_RegBatchMsgs / _NumOfSteps),
            _StepInterval = leo_math:ceiling(_RegInterval / _NumOfSteps),
            {ok, {_StepBatchOfMsgs,_StepInterval}}
        end).
