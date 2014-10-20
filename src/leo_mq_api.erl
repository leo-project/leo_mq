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

-export([new/2, new/3, publish/3, status/1]).

-define(APP_NAME,      'leo_mq').
-define(DEF_DB_MODULE, 'leo_mq_eleveldb'). % Not used in anywhere.

-define(DEF_BACKEND_DB_PROCS, 3).
-define(DEF_BACKEND_DB,      'bitcask').
-define(DEF_DB_ROOT_PATH,    "mq"  ).

-define(DEF_CONSUME_MAX_INTERVAL, 3000).
-define(DEF_CONSUME_MIN_INTERVAL, 1000).
-define(DEF_CONSUME_NUM_OF_BATCH_PROC, 1).


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
                 consumer_id  =
                     list_to_atom(
                       lists:append([atom_to_list(Id), "_consumer"])),
                 mod_callback = leo_misc:get_value(?MQ_PROP_MOD,          Props, Mod),
                 db_name      = leo_misc:get_value(?MQ_PROP_DB_NAME,      Props, ?DEF_BACKEND_DB),
                 db_procs     = leo_misc:get_value(?MQ_PROP_DB_PROCS,     Props, ?DEF_BACKEND_DB_PROCS),
                 root_path    = leo_misc:get_value(?MQ_PROP_ROOT_PATH,    Props, ?DEF_DB_ROOT_PATH),
                 num_of_batch_processes = leo_misc:get_value(?MQ_PROP_NUM_OF_BATCH_PROC, Props, ?DEF_CONSUME_NUM_OF_BATCH_PROC),
                 max_interval = leo_misc:get_value(?MQ_PROP_MAX_INTERVAL, Props, ?DEF_CONSUME_MAX_INTERVAL),
                 min_interval = leo_misc:get_value(?MQ_PROP_MIN_INTERVAL, Props, ?DEF_CONSUME_MIN_INTERVAL)
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


%% @doc Retrieve a current state from the queue
%%
-spec(status(Id) ->
             {ok, list()} when Id::atom()).
status(Id) ->
    leo_mq_publisher:status(Id).

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
