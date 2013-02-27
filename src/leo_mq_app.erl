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
%% Leo MQ - Application
%% @doc
%% @end
%%======================================================================
-module(leo_mq_app).

-author('Yosuke Hara').

-behaviour(application).

-include_lib("eunit/include/eunit.hrl").

-export([start/2, stop/1, profile_output/0]).

%%----------------------------------------------------------------------
%% Application behaviour callbacks
%%----------------------------------------------------------------------
start(_Type, _Args) ->
    consider_profiling(),
    Res = leo_mq_sup:start_link(),
    after_proc(Res).

stop(_State) ->
    ok.


-spec profile_output() -> ok.
profile_output() ->
    eprof:stop_profiling(),
    eprof:log("leo_mq.procs.profile"),
    eprof:analyze(procs),
    eprof:log("leo_mq.total.profile"),
    eprof:analyze(total).


%% @doc
%% @private
-spec consider_profiling() -> profiling | not_profiling | {error, any()}.
consider_profiling() ->
    case application:get_env(leo_mq, profile) of
        {ok, true} ->
            {ok, _Pid} = eprof:start(),
            eprof:start_profiling([self()]);
        _ ->
            not_profiling
    end.


%% @doc After sup launch processing
%% @private
-spec(after_proc({ok, pid()} | {error, any()}) ->
             {ok, pid()} | {error, any()}).
after_proc({ok, RefSup}) ->
    %% Launch backend-db's sup
    %%   under the leo_object_storage_sup
    ChildSpec = {leo_backend_db_sup,
                 {leo_backend_db_sup, start_link, []},
                 permanent, 2000, supervisor, [leo_backend_db_sup]},

    case supervisor:start_child(RefSup, ChildSpec) of
        {ok, Pid} ->
            ok = application:set_env(leo_mq, backend_db_sup_ref, Pid);
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "start_child/2"},
                                    {line, ?LINE}, {body, "Could NOT start backend-db sup"}]),
            exit(Cause)
    end,
    {ok, RefSup};

after_proc(Error) ->
    Error.

