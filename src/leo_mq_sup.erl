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
%% Leo MQ - Supervisor.
%% @doc leo_mq's supervisor
%% @reference https://github.com/leo-project/leo_mq/blob/master/src/leo_mq_sup.erl
%% @end
%%======================================================================
-module(leo_mq_sup).

-behaviour(supervisor).

-include_lib("eunit/include/eunit.hrl").

-export([start_link/0,
         stop/0,
         init/1]).

%%-----------------------------------------------------------------------
%% External API
%%-----------------------------------------------------------------------
%% @doc Creates a supervisor process as part of a supervision tree
%% @end
start_link() ->
    Res = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    after_proc(Res).


%% @doc Stop process
%% @end
stop() ->
    case whereis(?MODULE) of
        Pid when is_pid(Pid) == true ->
            List = supervisor:which_children(Pid),
            ok = close_db(List),
            ok;
        _ -> not_started
    end.

%% ---------------------------------------------------------------------
%% Callbacks
%% ---------------------------------------------------------------------
%% @doc supervisor callback - Module:init(Args) -> Result
%% @end
init([]) ->
    {ok, {{one_for_one, 5, 60}, []}}.


%% ---------------------------------------------------------------------
%% Inner Function(s)
%% ---------------------------------------------------------------------
%% @doc Processing of the sup after launched
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
                                               [{module, ?MODULE_STRING}, {function, "after_proc/2"},
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


%% @doc Close the internal databases
%% @private
close_db([]) ->
    ok;
close_db([{Id,_Pid, worker, ['leo_mq_server' = Mod|_]}|T]) ->
    ok = Mod:close(Id),
    close_db(T);
close_db([_|T]) ->
    close_db(T).
