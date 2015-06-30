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
%%======================================================================
-module(leo_mq_invoker).

-author('Yosuke Hara').

-behaviour(gen_server).

-include("leo_mq.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/4,
         stop/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-ifdef(TEST).
-define(CURRENT_TIME, 65432100000).
-else.
-define(CURRENT_TIME, leo_date:now()).
-endif.

-define(DEF_TIMEOUT, timer:seconds(10)).
-define(DEF_AFTER_NOT_FOUND_INTERVAL_MIN,  5000).
-define(DEF_AFTER_NOT_FOUND_INTERVAL_MAX, 10000).

-record(state, {id :: atom(),
                publisher_id :: atom(),
                db_procs = 1 :: pos_integer(),
                timeout = ?DEF_TIMEOUT :: pos_integer()
               }).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Creates the gen_server process as part of a supervision tree
-spec(start_link(Id, PublisherId, DbProcs, Timeout) ->
             {ok,pid()} | ignore | {error, any()} when Id::atom(),
                                                       PublisherId::atom(),
                                                       DbProcs::pos_integer(),
                                                       Timeout::pos_integer()).
start_link(Id, PublisherId, DbProcs, Timeout) ->
    gen_server:start_link({local, Id}, ?MODULE,
                          [Id, PublisherId, DbProcs, Timeout], []).

%% @doc Close the process
-spec(stop(Id) ->
             ok | {error, any()} when Id::atom()).
stop(Id) ->
    gen_server:call(Id, stop, ?DEF_TIMEOUT).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% @doc gen_server callback - Module:init(Args) -> Result
init([Id, PublisherId, DbProcs, Timeout]) ->
    {ok, #state{id = Id,
                publisher_id = PublisherId,
                db_procs = DbProcs,
                timeout = Timeout}, Timeout}.

handle_call(stop, _From, State) ->
    {stop, shutdown, ok, State};

handle_call(_Msg, _From, #state{timeout = Timeout} = State) ->
    {reply, ok, State, Timeout}.

handle_cast(_Msg, #state{timeout = Timeout} = State) ->
    {noreply, State, Timeout}.

%% @doc gen_server callback - Module:handle_info(Info, State) -> Result
handle_info(timeout, #state{publisher_id = PublisherId,
                            db_procs = DbProcs,
                            timeout = Timeout} = State) ->
    [leo_mq_consumer:run(CId, true) ||
        CId <- [?consumer_id(PublisherId, N) || N <- lists:seq(1, DbProcs)]],
    {noreply, State, Timeout};
handle_info(_Info, #state{timeout = Timeout} = State) ->
    {noreply, State, Timeout}.


%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% <p>
%% gen_server callback - Module:terminate(Reason, State)
%% </p>
terminate(_Reason,_State) ->
    ok.


%% @doc Convert process state when code is changed
%% <p>
%% gen_server callback - Module:code_change(OldVsn, State, Extra) -> {ok, NewState} | {error, Reason}.
%% </p>
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
