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
%% Leo MQ - Supervisor.
%% @doc
%% @end
%%======================================================================
-module(leo_mq_sup).

-author('Yosuke Hara').

-behaviour(supervisor).

-include_lib("eunit/include/eunit.hrl").

-export([start_link/0,
         stop/0,
         init/1]).

%%-----------------------------------------------------------------------
%% External API
%%-----------------------------------------------------------------------
%% @spec () -> ok
%% @doc start link...
%% @end
start_link() ->
    Res = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    after_proc(Res).


%% @spec () -> ok |
%%             not_started
%% @doc stop process.
%% @end
stop() ->
    case whereis(?MODULE) of
        Pid when is_pid(Pid) == true ->
            stop(Pid),
            ok;
        _ -> not_started
    end.

stop(Pid) ->
    List = supervisor:which_children(Pid),
    Len  = length(List),

    ok = terminate_children(List),
    timer:sleep(Len * 100),
    exit(Pid, shutdown),
    ok.

%% ---------------------------------------------------------------------
%% Callbacks
%% ---------------------------------------------------------------------
%% @spec (Params) -> ok
%% @doc stop process.
%% @end
%% @private
init([]) ->
    {ok, {{one_for_one, 5, 60}, []}}.


%% ---------------------------------------------------------------------
%% Inner Function(s)
%% ---------------------------------------------------------------------
%% @doc After sup launch processing
%% @private
-spec(after_proc({ok, pid()} | {error, any()}) ->
             {ok, pid()} | {error, any()}).
after_proc({ok, RefSup}) ->
    %% Launch backend-db's sup
    %%   under the leo_mq_sup
    RefBDBSup =
        case whereis(leo_backend_db_sup) of
            undefined ->
                ChildSpec = {leo_backend_db_sup,
                             {leo_backend_db_sup, start_link, []},
                             permanent, 2000, supervisor, [leo_backend_db_sup]},
                case supervisor:start_child(RefSup, ChildSpec) of
                    {ok, Pid} ->
                        Pid;
                    {error, Cause} ->
                        error_logger:error_msg("~p,~p,~p,~p~n",
                                               [{module, ?MODULE_STRING}, {function, "start_child/2"},
                                                {line, ?LINE}, {body, "Could NOT start backend-db sup"}]),
                        exit(Cause)
                end;
            Pid ->
                Pid
        end,
    ok = application:set_env(leo_mq, backend_db_sup_ref, RefBDBSup),
    {ok, RefSup};

after_proc(Error) ->
    Error.

terminate_children([]) ->
        ok;
terminate_children([{_Id,_Pid, supervisor, [Mod|_]}|T]) ->
        Mod:stop(),
        terminate_children(T);
terminate_children([{Id,_Pid, worker, [Mod|_]}|T]) ->
        Mod:stop(Id),
        terminate_children(T);
terminate_children([_|T]) ->
        terminate_children(T).
