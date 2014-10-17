%%======================================================================
%%
%% Leo Object Storage
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
%% @doc FSM of the data-compaction worker, which handles removing unnecessary objects from a object container.
%% @reference https://github.com/leo-project/leo_object_storage/blob/master/src/leo_compact_fsm_controller.erl
%% @end
%%======================================================================
-module(leo_mq_fsm_worker).

-author('Yosuke Hara').

-behaviour(gen_fsm).

-include("leo_mq.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/4, stop/1]).
-export([run/1, run/3,
         suspend/1,
         resume/1,
         finish/1,
         state/2]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4,
         format_status/2]).

-export([idling/2, idling/3,
         running/2, running/3,
         suspending/2, suspending/3]).

-compile(nowarn_deprecated_type).
-define(DEF_TIMEOUT, timer:seconds(30)).

-record(event_info, {
          id :: atom(),
          event = ?EVENT_RUN :: event_of_compaction(),
          controller_pid :: pid(),
          client_pid     :: pid(),
          callback :: function()
         }).

-record(state, {
          id :: atom(),
          status = ?ST_IDLING   :: state_of_compaction(),
          cntl_pid :: pid(),
          is_locked = false     :: boolean(),
          waiting_time = 0      :: non_neg_integer(),
          start_datetime = 0 :: non_neg_integer()
         }).


%%====================================================================
%% API
%%====================================================================
%% @doc Creates a gen_fsm process as part of a supervision tree
-spec(start_link(Id, ObjStorageId, MetaDBId, LoggerId) ->
             {ok, pid()} | {error, any()} when Id::atom(),
                                               ObjStorageId::atom(),
                                               MetaDBId::atom(),
                                               LoggerId::atom()).
start_link(Id, ObjStorageId, MetaDBId, LoggerId) ->
    gen_fsm:start_link({local, Id}, ?MODULE,
                       [Id, ObjStorageId, MetaDBId, LoggerId], []).


%% @doc Stop this server
%%
-spec(stop(Id) ->
             ok when Id::atom()).
stop(Id) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "stop/1"},
                           {line, ?LINE}, {body, Id}]),
    gen_fsm:sync_send_all_state_event(Id, stop, ?DEF_TIMEOUT).


%% @doc Run the process
%%
-spec(run(Id) ->
             ok | {error, any()} when Id::atom()).

run(Id) ->
    gen_fsm:send_event(Id, #event_info{event = ?EVENT_RUN}).

-spec(run(Id, ControllerPid, CallbackFun) ->
             ok | {error, any()} when Id::atom(),
                                      ControllerPid::pid(),
                                      CallbackFun::function()).
run(Id, ControllerPid, CallbackFun) ->
    gen_fsm:sync_send_event(Id, #event_info{event = ?EVENT_RUN,
                                            controller_pid = ControllerPid,
                                            callback = CallbackFun}, ?DEF_TIMEOUT).


%% @doc Retrieve an object from the object-storage
%%
-spec(suspend(Id) ->
             ok | {error, any()} when Id::atom()).
suspend(Id) ->
    gen_fsm:send_event(Id, #event_info{event = ?EVENT_SUSPEND}).


%% @doc Remove an object from the object-storage - (logical-delete)
%%
-spec(resume(Id) ->
             ok | {error, any()} when Id::atom()).
resume(Id) ->
    gen_fsm:sync_send_event(Id, #event_info{event = ?EVENT_RESUME}, ?DEF_TIMEOUT).


%% @doc Remove an object from the object-storage - (logical-delete)
%%
-spec(finish(Id) ->
             ok | {error, any()} when Id::atom()).
finish(Id) ->
    gen_fsm:send_event(Id, #event_info{event = ?EVENT_FINISH}).


%% @doc Retrieve the storage stats specfied by Id
%%      which contains number of objects and so on.
%%
-spec(state(Id, Client) ->
             ok | {error, any()} when Id::atom(),
                                      Client::pid()).
state(Id, Client) ->
    gen_fsm:send_event(Id, #event_info{event = state,
                                       client_pid = Client}).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% @doc Initiates the server
%%
init([Id]) ->
    {ok, ?ST_IDLING, #state{id = Id}}.

%% @doc Handle events
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @doc Handle 'status' event
handle_sync_event(state, _From, StateName, State) ->
    {reply, {ok, StateName}, StateName, State};

%% @doc Handle 'stop' event
handle_sync_event(stop, _From, _StateName, Status) ->
    {stop, shutdown, ok, Status}.


%% @doc Handling all non call/cast messages
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.


%% @doc This function is called by a gen_server when it is about to
%%      terminate. It should be the opposite of Module:init/1 and do any necessary
%%      cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
terminate(Reason, _StateName, _State) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "terminate/2"},
                           {line, ?LINE}, {body, Reason}]),
    ok.

%% @doc Convert process state when code is changed
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% @doc This function is called by a gen_fsm when it should update
%%      its internal state data during a release upgrade/downgrade
format_status(_Opt, [_PDict, State]) ->
    State.


%%====================================================================
%% CALLBACKS
%%====================================================================
%% @doc State of 'idle'
%%
-spec(idling(EventInfo, From, State) ->
             {next_state, ?ST_IDLING | ?ST_RUNNING, State} when EventInfo::#event_info{},
                                                                From::{pid(),Tag::atom()},
                                                                State::#state{}).
idling(#event_info{event = ?EVENT_RUN,
                   controller_pid = ControllerPid,
                   callback = _CallbackFun}, From, #state{id = Id} = State) ->
    NextStatus = ?ST_RUNNING,
    State_1 = State#state{cntl_pid = ControllerPid,
                          status        = NextStatus,
                          start_datetime = leo_date:now()},

    case prepare(State_1) of
        {ok, State_2} ->
            gen_fsm:reply(From, ok),
            ok = run(Id),
            {next_state, NextStatus, State_2};
        {{error, Cause},_State} ->
            gen_fsm:reply(From, {error, Cause}),
            {next_state, ?ST_IDLING, State_1}
    end;
idling(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#state{status = NextStatus}}.

-spec(idling(EventInfo, State) ->
             {next_state, ?ST_IDLING, State} when EventInfo::#event_info{},
                                                  State::#state{}).
idling(#event_info{event = ?EVENT_STATE,
                   client_pid = Client}, State) ->
    NextStatus = ?ST_IDLING,
    erlang:send(Client, NextStatus),
    {next_state, NextStatus, State#state{status = NextStatus}};
idling(_, State) ->
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#state{status = NextStatus}}.


%% @doc State of 'running'
-spec(running(EventInfo, State) ->
             {next_state, ?ST_RUNNING, State} when EventInfo::#event_info{},
                                                   State::#state{}).
running(#event_info{event = ?EVENT_RUN},
        #state{id = Id,
               cntl_pid = _CntlPid} = State) ->
    NextStatus = ?ST_RUNNING,
    State_3 =
        case catch execute(State) of
            %% Execute the data-compaction repeatedly
            {ok, {next, State_1}} ->
                ok = run(Id),
                State_1;
            %% An unxepected error has occured
            {'EXIT', Cause} ->
                ok = finish(Id),
                {_,State_2} = after_execute({error, Cause}, State),
                State_2;
            %% Reached end of the object-container
            {ok, {eof, State_1}} ->
                ok = finish(Id),
                {_,State_2} = after_execute(ok, State_1),
                State_2;
            %% An epected error has occured
            {{error, Cause}, State_1} ->
                ok = finish(Id),
                {_,State_2} = after_execute({error, Cause}, State_1),
                State_2
        end,
    {next_state, NextStatus, State_3#state{status = NextStatus}};

running(#event_info{event = ?EVENT_SUSPEND}, State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#state{status = NextStatus}};

running(#event_info{event = ?EVENT_FINISH}, #state{id   = Id,
                                                   cntl_pid = CntlPid} = State) ->
    %% Notify a message to the compaction-manager
    erlang:send(CntlPid, {finish, Id}),
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#state{status = NextStatus,
                                         start_datetime = 0}};
running(#event_info{event = ?EVENT_STATE,
                    client_pid = Client}, State) ->
    NextStatus = ?ST_RUNNING,
    erlang:send(Client, NextStatus),
    {next_state, NextStatus, State#state{status = NextStatus}};
running(_, State) ->
    NextStatus = ?ST_RUNNING,
    {next_state, NextStatus, State#state{status = NextStatus}}.


-spec(running( _, _, #state{}) ->
             {next_state, ?ST_SUSPENDING|?ST_RUNNING, #state{}}).
running(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?ST_RUNNING,
    {next_state, NextStatus, State#state{status = NextStatus}}.


%% @doc State of 'suspend'
%%
-spec(suspending(EventInfo, State) ->
             {next_state, ?ST_SUSPENDING, State} when EventInfo::#event_info{},
                                                      State::#state{}).
suspending(#event_info{event = ?EVENT_RUN}, State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#state{status = NextStatus}};
suspending(#event_info{event = ?EVENT_STATE,
                       client_pid = Client}, State) ->
    NextStatus = ?ST_SUSPENDING,
    erlang:send(Client, NextStatus),
    {next_state, NextStatus, State#state{status = NextStatus}};
suspending(_, State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#state{status = NextStatus}}.

-spec(suspending(EventInfo, From, State) ->
             {next_state, ?ST_SUSPENDING | ?ST_RUNNING, State} when EventInfo::#event_info{},
                                                                    From::{pid(),Tag::atom()},
                                                                    State::#state{}).
suspending(#event_info{event = ?EVENT_RESUME}, From, #state{id = Id} = State) ->
    gen_fsm:reply(From, ok),
    ok = run(Id),

    NextStatus = ?ST_RUNNING,
    {next_state, NextStatus, State#state{status = NextStatus}};

suspending(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#state{status = NextStatus}}.


%%--------------------------------------------------------------------
%% Inner Functions
%%--------------------------------------------------------------------
%% @doc compact objects from the object-container on a independent process.
%% @private
-spec(prepare(State) ->
             {ok, State} | {{error, any()}, State} when State::#state{}).
prepare(State) ->
    {ok, State}.


%% @doc Reduce unnecessary objects from object-container.
%% @private
-spec(execute(State) ->
             {ok, State} | {{error, any()}, State} when State::#state{}).
execute(State) ->
    {ok, State}.

%% @private
after_execute(Ret, State) ->
    {Ret, State}.
