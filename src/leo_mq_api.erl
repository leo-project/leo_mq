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
%% @doc
%% @end
%%======================================================================
-module(leo_mq_api).

-author('Yosuke Hara').

-include("leo_mq.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([new/2, new/3, publish/3, status/1]).

-define(APP_NAME,      'leo_mq').
-define(DEF_DB_MODULE, 'leo_mq_eleveldb').

-define(DEF_BACKEND_DB_PROCS, 3).
-define(DEF_BACKEND_DB,      'bitcask').
-define(DEF_DB_ROOT_PATH,    "mq"  ).

-define(DEF_CONSUME_MAX_INTERVAL, 3000).
-define(DEF_CONSUME_MIN_INTERVAL, 1000).
-define(DEF_CONSUME_NUM_OF_BATCH_PROC, 1).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc
%%
-spec(new(atom(), list()) ->
             ok | {error, any()}).
new(Id, PropLists) when is_list(PropLists) == true ->
    case leo_misc:get_value(?MQ_PROP_MOD, PropLists, undefined) of
        undefined ->
            {error, badarg};
        Mod ->
            new(Id,
                #mq_properties
                {module       = Mod,
                 function     = leo_misc:get_value(?MQ_PROP_FUN,          PropLists, ?MQ_SUBSCRIBE_FUN),
                 db_name      = leo_misc:get_value(?MQ_PROP_DB_NAME,      PropLists, ?DEF_BACKEND_DB),
                 db_procs     = leo_misc:get_value(?MQ_PROP_DB_PROCS,     PropLists, ?DEF_BACKEND_DB_PROCS),
                 root_path    = leo_misc:get_value(?MQ_PROP_ROOT_PATH,    PropLists, ?DEF_DB_ROOT_PATH),
                 num_of_batch_processes = leo_misc:get_value(?MQ_PROP_NUM_OF_BATCH_PROC, PropLists, ?DEF_CONSUME_NUM_OF_BATCH_PROC),
                 max_interval = leo_misc:get_value(?MQ_PROP_MAX_INTERVAL, PropLists, ?DEF_CONSUME_MAX_INTERVAL),
                 min_interval = leo_misc:get_value(?MQ_PROP_MIN_INTERVAL, PropLists, ?DEF_CONSUME_MIN_INTERVAL)
                })
    end;

new(Id, Props) ->
    new(leo_mq_sup, Id, Props).

new(RefSup, Id, Props0) ->
    Props1 =
        case is_list(Props0) of
            true ->
                #mq_properties
                    {module       = leo_misc:get_value(?MQ_PROP_MOD,          Props0, undefined),
                     function     = leo_misc:get_value(?MQ_PROP_FUN,          Props0, ?MQ_SUBSCRIBE_FUN),
                     db_name      = leo_misc:get_value(?MQ_PROP_DB_NAME,      Props0, ?DEF_BACKEND_DB),
                     db_procs     = leo_misc:get_value(?MQ_PROP_DB_PROCS,     Props0, ?DEF_BACKEND_DB_PROCS),
                     root_path    = leo_misc:get_value(?MQ_PROP_ROOT_PATH,    Props0, ?DEF_DB_ROOT_PATH),
                     num_of_batch_processes = leo_misc:get_value(?MQ_PROP_NUM_OF_BATCH_PROC, Props0, ?DEF_CONSUME_NUM_OF_BATCH_PROC),
                     max_interval = leo_misc:get_value(?MQ_PROP_MAX_INTERVAL, Props0, ?DEF_CONSUME_MAX_INTERVAL),
                     min_interval = leo_misc:get_value(?MQ_PROP_MIN_INTERVAL, Props0, ?DEF_CONSUME_MIN_INTERVAL)
                    };
            false ->
                Props0
        end,

    ChildSpec = {Id,
                 {leo_mq_server, start_link, [Id, Props1]},
                 permanent, 2000, worker, [leo_mq_server]},

    case supervisor:start_child(RefSup, ChildSpec) of
        {ok, _Pid} ->
            ok;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "new/2"},
                                    {line, ?LINE}, {body, Cause}]),
            case leo_mq_sup:stop() of
                ok ->
                    exit(invalid_launch);
                not_started ->
                    exit(noproc)
            end
    end.


%% @doc publish a message into the queue.
%%
-spec(publish(atom(), binary(), binary()) ->
             ok | {error, any()}).
publish(Id, KeyBin, MessageBin) ->
    leo_mq_server:publish(Id, KeyBin, MessageBin).


%% @doc get state from the queue.
%%
-spec(status(atom()) ->
             {ok, list()}).
status(Id) ->
    leo_mq_server:status(Id).

%%--------------------------------------------------------------------
%% INNTERNAL FUNCTIONS
%%--------------------------------------------------------------------

