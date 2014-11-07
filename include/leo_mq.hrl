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
%% Leo MQ
%% @doc
%% @end
%%======================================================================
-define(MQ_LOG_ID,                 'queue').
-define(MQ_LOG_FILE_NAME,          "queue").
-define(MQ_PROP_MOD,               'module').
-define(MQ_PROP_FUN,               'function').
-define(MQ_PROP_DB_NAME,           'db_name').
-define(MQ_PROP_DB_PROCS,          'db_procs').
-define(MQ_PROP_ROOT_PATH,         'root_path').
-define(MQ_PROP_NUM_OF_BATCH_PROC, 'num_of_batch_processes').
-define(MQ_PROP_MAX_INTERVAL,      'max_interval').
-define(MQ_PROP_MIN_INTERVAL,      'min_interval').
-define(MQ_SUBSCRIBE_FUN,          'subscribe').


-record(mq_properties, {
          publisher_id :: atom(),
          consumer_id  :: atom(),
          mod_callback :: module(),

          db_name           :: atom(),
          db_procs = 1      :: integer(),
          root_path = []    :: string(),
          mqdb_id           :: atom(),
          mqdb_path = []    :: string(),

          max_interval = 1  :: integer(),
          min_interval = 1  :: integer(),
          num_of_batch_processes = 1 :: pos_integer()
         }).

-record(mq_log, {
          type             :: atom(),
          requested_at = 0 :: integer(),
          format  = []     :: string(),
          message = []     :: string()}).


-define(ST_IDLING,     'idling').
-define(ST_RUNNING,    'running').
-define(ST_SUSPENDING, 'suspending').
-type(state_of_mq() :: ?ST_IDLING     |
                       ?ST_RUNNING    |
                       ?ST_SUSPENDING).

-record(mq_state, {
          id :: atom(),
          alias :: atom(),
          state :: state_of_mq(),
          num_of_messages = 0 :: non_neg_integer()
         }).


-define(EVENT_RUN,      'run').
-define(EVENT_DIAGNOSE, 'diagnose').
-define(EVENT_LOCK,     'lock').
-define(EVENT_SUSPEND,  'suspend').
-define(EVENT_RESUME,   'resume').
-define(EVENT_FINISH,   'finish').
-define(EVENT_STATE,    'state').
-type(event_of_compaction() ::?EVENT_RUN      |
                              ?EVENT_DIAGNOSE |
                              ?EVENT_LOCK     |
                              ?EVENT_SUSPEND  |
                              ?EVENT_RESUME   |
                              ?EVENT_FINISH   |
                              ?EVENT_STATE).


-define(DEF_CHECK_MAX_INTERVAL_1, timer:seconds(1)).
-define(DEF_CHECK_MIN_INTERVAL_1, timer:seconds(0)).
-define(DEF_CHECK_MAX_INTERVAL_2, timer:seconds(30)).
-define(DEF_CHECK_MIN_INTERVAL_2, timer:seconds(10)).

-define(DEF_CONSUMER_SUFFIX, "_consumer").

-define(consumer_id(_PubId),
        list_to_atom(
          lists:append([atom_to_list(_PubId), ?DEF_CONSUMER_SUFFIX]))).

-define(publisher_id(_ConsumerId),
        begin
            _StrId = atom_to_list(_ConsumerId),
            list_to_atom(
              string:substr(_StrId, 1,
                            string:str(_StrId, ?DEF_CONSUMER_SUFFIX) - 1))
        end).

%% Retrieve the backend-db info
-define(DEF_DB_PATH_INDEX,   "index"  ).
-define(DEF_DB_PATH_MESSAGE, "message").

-define(backend_db_info(Id, RootPath),
        begin
            MQDBMessageId = list_to_atom(atom_to_list(Id) ++ "_message"),
            NewRootPath = case (string:len(RootPath) == string:rstr(RootPath, "/")) of
                              true  -> RootPath;
                              false ->  RootPath ++ "/"
                          end,
            MQDBMessagePath = NewRootPath ++ ?DEF_DB_PATH_MESSAGE,
            {MQDBMessageId, MQDBMessagePath}
        end).
