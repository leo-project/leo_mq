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
%% Leo MQ  - Behaviour.
%% @doc
%% @end
%%======================================================================
-module(leo_mq_behaviour).

-author('Yosuke Hara').

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     %% init() -> ok.
     {init, 0},

     %% hande_call({publish, Id, reply}) -> ok.
     %% hande_call({consume, Id, message_bin}) -> ok.
     {handle_call, 1}
    ];
behaviour_info(_Other) ->
    undefined.
