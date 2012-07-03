%%======================================================================
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
%% ---------------------------------------------------------------------
%% Leo MQ
%% @doc
%% @end
%%======================================================================
-define(MQ_LOG_ID,            'queue').
-define(MQ_LOG_FILE_NAME,     "queue").
-define(MQ_PROP_MOD,          'module').
-define(MQ_PROP_FUN,          'function').
-define(MQ_PROP_DB_NAME,      'db_name').
-define(MQ_PROP_DB_PROCS,     'db_procs').
-define(MQ_PROP_ROOT_PATH,    'root_path').
-define(MQ_PROP_MAX_INTERVAL, 'max_interval').
-define(MQ_PROP_MIN_INTERVAL, 'min_interval').
-define(MQ_SUBSCRIBE_FUN,     'subscribe').


-record(mq_properties, {module       :: atom(),
                        function     :: atom(),
                        db_name      :: atom(),
                        db_procs     :: integer(),
                        root_path    :: string(),
                        max_interval :: integer(),
                        min_interval :: integer()
                       }).

-record(mq_log,
        {type         :: atom(),
         requested_at :: integer(),
         format       :: string(),
         message      :: string()}).

