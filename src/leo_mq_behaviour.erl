%%======================================================================
%%
%% Leo MQ
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
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
%% @doc leo_mq's behaviour
%% @reference https://github.com/leo-project/leo_mq/blob/master/src/leo_mq_behaviour.erl
%% @end
%%======================================================================
-module(leo_mq_behaviour).

-callback(init() ->
                 ok | {error, any()}).
-callback(handle_call(tuple()) ->
                 ok | {error, any()}).
-callback(handle_call(atom(), atom(), any()) ->
                 ok | {error, any()}).
