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
%% Leo MQ - Server
%% @doc The gen_server process for the process of a mq's consumer as part of a supervision tree
%% @reference https://github.com/leo-project/leo_mq/blob/master/src/leo_mq_consumer.erl
%% @end
%%======================================================================
-module(leo_mq_consumer).

-author('Yosuke Hara').

-behaviour(gen_fsm).

-include("leo_mq.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/3, stop/1]).
-export([run/1,
         suspend/1,
         resume/1,
         state/1,
         incr_waiting_time/1,
         decr_waiting_time/1
        ]).

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
         suspending/2, suspending/3,
         defer_consume/3
        ]).

-compile(nowarn_deprecated_type).
-define(DEF_TIMEOUT, timer:seconds(30)).

-record(event_info, {
          id :: atom(),
          event = ?EVENT_RUN :: event_of_compaction()
         }).

-record(state, {
          id :: atom(),
          status = ?ST_IDLING :: state_of_mq(),
          publisher_id        :: atom(),
          mq_properties = #mq_properties{} :: #mq_properties{},
          waiting_time = 0    :: non_neg_integer(),
          start_datetime = 0  :: non_neg_integer()
         }).


%%====================================================================
%% API
%%====================================================================
%% @doc Creates a gen_fsm process as part of a supervision tree
-spec(start_link(Id, PublisherId, Props) ->
             {ok, pid()} | {error, any()} when Id::atom(),
                                               PublisherId::atom(),
                                               Props::#mq_properties{}).
start_link(Id, PublisherId, Props) ->
    gen_fsm:start_link({local, Id}, ?MODULE,
                       [Id, PublisherId, Props], []).


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


%% @doc Retrieve the storage stats specfied by Id
%%      which contains number of objects and so on.
-spec(state(Id) ->
             {ok, state_of_mq()} when Id::atom()).
state(Id) ->
    gen_fsm:sync_send_event(Id, #event_info{event = ?EVENT_STATE}, ?DEF_TIMEOUT).


%% @doc Increase waiting-time in order to down load of processing
-spec(incr_waiting_time(Id) ->
             {ok, state_of_mq()} when Id::atom()).
incr_waiting_time(Id) ->
    gen_fsm:send_event(Id, #event_info{event = ?EVENT_INCR_WT}).


%% @doc Decrease waiting-time in order to up consuming speed
-spec(decr_waiting_time(Id) ->
             {ok, state_of_mq()} when Id::atom()).
decr_waiting_time(Id) ->
    gen_fsm:send_event(Id, #event_info{event = ?EVENT_DECR_WT}).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% @doc Initiates the server
%%
init([Id, PublisherId, Props]) ->
    _ = defer_consume(Id, ?DEF_CHECK_MAX_INTERVAL_1, ?DEF_CHECK_MIN_INTERVAL_1),
    {ok, ?ST_IDLING, #state{id = Id,
                            publisher_id  = PublisherId,
                            mq_properties = Props}}.

%% @doc Handle events
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @doc Handle 'status' event
handle_sync_event(state, _From, StateName, State) ->
    {reply, {ok, StateName}, StateName, State};

%% @doc Handle 'stop' event
handle_sync_event(stop, _From, _StateName, State) ->
    {stop, shutdown, ok, State}.


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
idling(#event_info{event = ?EVENT_RUN}, From, #state{id = Id} = State) ->
    NextStatus = ?ST_RUNNING,
    State_1 = State#state{status = NextStatus,
                          start_datetime = leo_date:now()},
    gen_fsm:reply(From, ok),
    ok = run(Id),
    {next_state, NextStatus, State_1};
idling(#event_info{event = ?EVENT_STATE}, From, State) ->
    NextStatus = ?ST_IDLING,
    gen_fsm:reply(From, {ok, NextStatus}),
    {next_state, NextStatus, State};
idling(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#state{status = NextStatus}}.

-spec(idling(EventInfo, State) ->
             {next_state, ?ST_IDLING, State} when EventInfo::#event_info{},
                                                  State::#state{}).
idling(#event_info{event = ?EVENT_RUN}, #state{id = Id} = State) ->
    NextStatus = ?ST_RUNNING,
    ok = run(Id),
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
               mq_properties = #mq_properties{
                                  max_interval = MaxInterval,
                                  min_interval = MinInterval}} = State) ->
    {NextStatus, State_2} =
        case catch consume(State) of
            %% Execute the data-compaction repeatedly
            ok ->
                Time = interval(MinInterval, MaxInterval),
                ok = timer:sleep(Time),
                ok = run(Id),
                {?ST_RUNNING,  State};
            %% Reached end of the object-container
            not_found ->
                {_,State_1} = after_execute(ok, State),
                {?ST_IDLING,  State_1};
            %% An unxepected error has occured
            {'EXIT', Cause} ->
                {_,State_1} = after_execute({error, Cause}, State),
                {?ST_IDLING,  State_1};
            %% An epected error has occured
            {error, Cause} ->
                {_,State_1} = after_execute({error, Cause}, State),
                {?ST_IDLING,  State_1}
        end,
    {next_state, NextStatus, State_2#state{status = NextStatus}};

running(#event_info{event = ?EVENT_SUSPEND}, State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#state{status = NextStatus}};

running(#event_info{event = ?EVENT_INCR_WT}, #state{id = Id,
                                                    mq_properties = #mq_properties{max_interval  = MaxWaitingTime,
                                                                                   step_interval = StepWaitingTime},
                                                    waiting_time  = WaitingTime} = State) ->
    WaitingTime_1 = WaitingTime + StepWaitingTime,
    {IsOverThreshold, WaitingTime_2} =
        case (WaitingTime_1 > MaxWaitingTime) of
            true  -> {true, MaxWaitingTime};
            false -> {false, WaitingTime_1}
        end,
    NextStatus = case IsOverThreshold of
                     true ->
                         ?ST_SUSPENDING;
                     false ->
                         ok = run(Id),
                         ?ST_RUNNING
                 end,
    {next_state, NextStatus, State#state{status = NextStatus,
                                         waiting_time = WaitingTime_2}};

running(#event_info{event = ?EVENT_DECR_WT}, #state{id = Id,
                                                    mq_properties = #mq_properties{min_interval  = MinWaitingTime,
                                                                                   step_interval = StepWaitingTime},
                                                    waiting_time  = WaitingTime} = State) ->
    WaitingTime_1 = WaitingTime - StepWaitingTime,
    WaitingTime_2 = case (WaitingTime_1 < MinWaitingTime) of
                        true ->
                            MinWaitingTime;
                        false ->
                            WaitingTime_1
                    end,
    ok = run(Id),
    NextStatus = ?ST_RUNNING,
    {next_state, NextStatus, State#state{status = NextStatus,
                                         waiting_time = WaitingTime_2}};

running(_, State) ->
    NextStatus = ?ST_RUNNING,
    {next_state, NextStatus, State#state{status = NextStatus}}.

-spec(running( _, _, #state{}) ->
             {next_state, ?ST_RUNNING, #state{}}).
running(#event_info{event = ?EVENT_STATE}, From, State) ->
    NextStatus = ?ST_RUNNING,
    gen_fsm:reply(From, {ok, NextStatus}),
    {next_state, NextStatus, State};
running(_, From, State) ->
    NextStatus = ?ST_RUNNING,
    gen_fsm:reply(From, {error, badstate}),
    {next_state, NextStatus, State}.


%% @doc State of 'suspend'
%%
-spec(suspending(EventInfo, State) ->
             {next_state, ?ST_SUSPENDING, State} when EventInfo::#event_info{},
                                                      State::#state{}).
suspending(#event_info{event = ?EVENT_RUN}, State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#state{status = NextStatus}};
suspending(#event_info{event = ?EVENT_STATE}, State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#state{status = NextStatus}};
suspending(#event_info{event = ?EVENT_INCR_WT}, State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#state{status = NextStatus}};
suspending(#event_info{event = ?EVENT_DECR_WT}, #state{id = Id,
                                                       mq_properties = #mq_properties{min_interval  = MinWaitingTime,
                                                                                      step_interval = StepWaitingTime},
                                                       waiting_time = WaitingTime} = State) ->
    WaitingTime_1 = decr_waiting_time_fun(WaitingTime, MinWaitingTime, StepWaitingTime),
    NextStatus = ?ST_RUNNING,
    timer:apply_after(
      timer:seconds(1), ?MODULE, run, [Id]),
    {next_state, NextStatus, State#state{status = NextStatus,
                                         waiting_time = WaitingTime_1}};
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
suspending(#event_info{event = ?EVENT_STATE}, From, State) ->
    NextStatus = ?ST_SUSPENDING,
    gen_fsm:reply(From, {ok, NextStatus}),
    {next_state, NextStatus, State#state{status = NextStatus}};
suspending(_Other, From, State) ->
    NextStatus = ?ST_SUSPENDING,
    gen_fsm:reply(From, {error, badstate}),
    {next_state, NextStatus, State}.


%%--------------------------------------------------------------------
%% Inner Functions
%%--------------------------------------------------------------------
%% @doc after processing of consumption messages
%% @private
after_execute(Ret, #state{id = Id} = State) ->
    _ = defer_consume(Id, ?DEF_CHECK_MAX_INTERVAL_2,
                      ?DEF_CHECK_MIN_INTERVAL_2),
    {Ret, State}.


%% @doc Consume a message
%%
-spec(consume(State) ->
             ok | not_found | {error, any()} when State::#state{}).
consume(#state{mq_properties = #mq_properties{
                                  publisher_id = PublisherId,
                                  mod_callback = Mod,
                                  mqdb_id = BackendMessage,
                                  num_of_batch_processes = NumOfBatchProcs
                                 }} = _State) ->
    consume(PublisherId, Mod, BackendMessage, NumOfBatchProcs).

%% @doc Consume a message
%% @private
-spec(consume(atom(), atom(), atom(), non_neg_integer()) ->
             ok | not_found | {error, any()}).
consume(_,_,_,0) ->
    ok;
consume(Id, Mod, BackendMessage, NumOfBatchProcs) ->
    try
        case leo_backend_db_api:first(BackendMessage) of
            {ok, {Key, Val}} ->
                %% Taking measure of queue-msg migration
                %% for previsous 1.2.0-pre1
                MsgTerm = binary_to_term(Val),
                MsgBin = case is_tuple(MsgTerm) of
                             true when is_integer(element(1, MsgTerm)) andalso
                                       is_binary(element(2, MsgTerm)) ->
                                 element(2, MsgTerm);
                             _ ->
                                 Val
                         end,
                erlang:apply(Mod, handle_call, [{consume, Id, MsgBin}]),
                ok = leo_backend_db_api:delete(BackendMessage, Key),
                consume(Id, Mod, BackendMessage, NumOfBatchProcs - 1);
            not_found = Cause ->
                Cause;
            Error ->
                Error
        end
    catch
        _: Why ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "consume_fun/5"},
                                    {line, ?LINE}, {body, Why}]),
            {error, Why}
    end.


%% @doc Defer a cosuming message
%%
-spec(defer_consume(atom(), pos_integer(), integer()) ->
             {ok, timer:tref()} | {error,_}).
defer_consume(Id, MaxInterval, MinInterval) ->
    Time = interval(MinInterval, MaxInterval),
    timer:apply_after(Time, ?MODULE, run, [Id]).


%% @doc Retrieve interval of the waiting proc
%% @private
-spec(interval(MinInterval, MaxInterval) ->
             Interval when MinInterval::non_neg_integer(),
                           MaxInterval::non_neg_integer(),
                           Interval::non_neg_integer()).
interval(MinInterval, MaxInterval) ->
    Interval_1 = random:uniform(MaxInterval),
    Interval_2 = case (Interval_1 < MinInterval) of
                     true  -> MinInterval;
                     false -> Interval_1
                 end,
    Interval_2.



%% @doc Decrease the waiting time
%% @private
-spec(decr_waiting_time_fun(WaitingTime, MinWaitingTime, StepWaitingTime) ->
             NewWaitingTime when WaitingTime::non_neg_integer(),
                                 MinWaitingTime::non_neg_integer(),
                                 StepWaitingTime::non_neg_integer(),
                                 NewWaitingTime::non_neg_integer()).
decr_waiting_time_fun(WaitingTime, MinWaitingTime, StepWaitingTime) ->

    WaitingTime_1 = WaitingTime - StepWaitingTime,
    case (WaitingTime_1 < MinWaitingTime) of
        true ->
            MinWaitingTime;
        false ->
            WaitingTime_1
    end.

