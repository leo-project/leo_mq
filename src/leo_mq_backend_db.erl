%%======================================================================
%%
%% Leo MQ
%%
%% Copyright (c) 2012 Rakuten, Inc.
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
%% Leo MQ - BackendDB
%% @doc
%% @end
%%======================================================================
-module(leo_mq_backend_db).

-author('Yosuke Hara').

-export([start/4, get/2, put/3, delete/2, first/1, status/1]).
-type(backend_db() :: bitcask | leveldb).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc start local-db.
%%
-spec(start(atom(), integer(), backend_db(), string()) ->
             ok | {error, any()}).
start(InstanceName, NumOfDBProcs, DBModule, DBRootPath) ->
    leo_backend_db_api:new(InstanceName, NumOfDBProcs, DBModule, DBRootPath).


%% @doc get a value.
%%
-spec(get(InstanceName::atom(), KeyBin::binary()) ->
             not_found | {ok, binary()} | {error, any()}).
get(InstanceName, KeyBin) ->
    leo_backend_db_api:get(InstanceName, KeyBin).


%% @doc get a value.
%%
-spec(first(InstanceName::atom()) ->
             not_found | {ok, list()} | {error, any()}).
first(InstanceName) ->
    leo_backend_db_api:first(InstanceName).


%% @doc put a value.
%%
-spec(put(InstanceName::atom(), KeyBin::binary(), ValueBin::binary()) ->
             ok | {error, any()}).
put(InstanceName, KeyBin, ValueBin) ->
    leo_backend_db_api:put(InstanceName, KeyBin, ValueBin).


%% @doc delete meta as data/file.
%%
-spec(delete(InstanceName::atom(), KeyBin::binary()) -> ok | {error, any()}).
delete(InstanceName, KeyBin) ->
    leo_backend_db_api:delete(InstanceName, KeyBin).


%% @doc status
%%
-spec(status(atom()) ->
             {ok, list()}).
status(InstanceName) ->
    leo_backend_db_api:status(InstanceName).

%%--------------------------------------------------------------------
%% INNER FUNCTIONS
%%--------------------------------------------------------------------

