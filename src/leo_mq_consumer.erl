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
%% Leo MQ - Server
%% @doc The gen_server process for the process of a mq's consumer as part of a supervision tree
%% @reference https://github.com/leo-project/leo_mq/blob/master/src/leo_mq_consumer.erl
%% @end
%%======================================================================
-module(leo_mq_consumer).

-behaviour(gen_fsm).

-include("leo_mq.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/4, stop/1]).
-export([run/1, run/2,
         suspend/1,
         resume/1,
         state/1,
         increase/1,
         decrease/1
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
-define(DEF_TIMEOUT, timer:seconds(5)).

-record(event_info, {
          id :: atom(),
          event = ?EVENT_RUN :: event_of_compaction(),
          is_force_exec = false :: boolean()
         }).

-record(state, {
          id :: atom(),
          status = ?ST_IDLING :: state_of_mq(),
          publisher_id        :: atom(),
          named_mqdb_pid      :: atom(),
          mq_properties = #mq_properties{} :: #mq_properties{},
          worker_seq_num = 0  :: non_neg_integer(),
          interval = 0        :: non_neg_integer(),
          batch_of_msgs = 0   :: non_neg_integer(),
          start_datetime = 0  :: non_neg_integer(),
          prev_proc_time = 0  :: non_neg_integer()
         }).


%%====================================================================
%% API
%%====================================================================
%% @doc Creates a gen_fsm process as part of a supervision tree
-spec(start_link(Id, PublisherId, Props, WorkerSeqNum) ->
             {ok, pid()} | {error, any()} when Id::atom(),
                                               PublisherId::atom(),
                                               Props::#mq_properties{},
                                               WorkerSeqNum::non_neg_integer()
                                                             ).
start_link(Id, PublisherId, Props, WorkerSeqNum) ->
    gen_fsm:start_link({local, Id}, ?MODULE,
                       [Id, PublisherId, Props, WorkerSeqNum], []).


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
    run(Id, false).

-spec(run(Id, IsForceExec) ->
             ok | {error, any()} when Id::atom(),
                                      IsForceExec::boolean()).
run(Id, IsForceExec) ->
    gen_fsm:send_event(Id, #event_info{event = ?EVENT_RUN,
                                       is_force_exec = IsForceExec}).


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


%% @doc Increase comsumption processing
-spec(increase(Id) ->
             ok when Id::atom()).
increase(Id) ->
    gen_fsm:send_event(Id, #event_info{event = ?EVENT_INCR}).


%% @doc Decrease comsumption processing
-spec(decrease(Id) ->
             ok when Id::atom()).
decrease(Id) ->
    gen_fsm:send_event(Id, #event_info{event = ?EVENT_DECR}).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% @doc Initiates the server
%%
init([Id, PublisherId,
      #mq_properties{mqdb_id = MqDbId,
                     regular_interval = Interval,
                     regular_batch_of_msgs = BatchOfMsgs} = Props, WorkerSeqNum]) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "init/1"},
                           {line, ?LINE}, {body, [{id, Id},
                                                  {publisher_id, PublisherId},
                                                  {interval, Interval},
                                                  {batch_of_msgs, BatchOfMsgs}]}]),
    _ = defer_consume(Id, ?DEF_CHECK_MAX_INTERVAL_1, ?DEF_CHECK_MIN_INTERVAL_1),
    NamedPid = list_to_atom(atom_to_list(MqDbId)
                            ++ "_"
                            ++ integer_to_list(WorkerSeqNum)),
    {ok, ?ST_IDLING, #state{id = Id,
                            publisher_id = PublisherId,
                            named_mqdb_pid = NamedPid,
                            mq_properties = Props,
                            worker_seq_num = WorkerSeqNum,
                            interval = Interval,
                            batch_of_msgs = BatchOfMsgs
                           }}.

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
handle_info(timeout, StateName, State) ->
    {next_state, StateName, State};
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
             {next_state, ?ST_IDLING | ?ST_RUNNING, State, non_neg_integer()}
                 when EventInfo::#event_info{},
                      From::{pid(), atom()},
                      State::#state{}).
idling(#event_info{event = ?EVENT_RUN}, From, #state{id = Id,
                                                     publisher_id = PublisherId,
                                                     batch_of_msgs = BatchOfMsgs,
                                                     interval = Interval} = State) ->
    NextStatus = ?ST_RUNNING,
    State_1 = State#state{status = ?ST_IDLING,
                          prev_proc_time = 0,
                          start_datetime = leo_date:now()},
    gen_fsm:reply(From, ok),
    ok = run(Id),
    ok = leo_mq_server:update_consumer_stats(PublisherId, NextStatus, BatchOfMsgs, Interval),
    {next_state, NextStatus, State_1};
idling(#event_info{event = ?EVENT_STATE}, From, #state{status = Status} = State) ->
    gen_fsm:reply(From, {ok, Status}),
    {next_state, ?ST_IDLING, State#state{status = Status}};
idling(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    {next_state, ?ST_IDLING, State#state{status = ?ST_IDLING}}.

-spec(idling(EventInfo, State) ->
             {next_state, ?ST_IDLING, State, non_neg_integer()} when EventInfo::#event_info{},
                                                                     State::#state{}).
idling(#event_info{event = ?EVENT_RUN}, #state{id = Id,
                                               publisher_id = PublisherId,
                                               batch_of_msgs = BatchOfMsgs,
                                               interval = Interval} = State) ->
    NextStatus = ?ST_RUNNING,
    ok = run(Id),
    ok = leo_mq_server:update_consumer_stats(PublisherId, NextStatus, BatchOfMsgs, Interval),
    {next_state, NextStatus, State#state{status = ?ST_IDLING}};

idling(#event_info{event = ?EVENT_INCR},
       #state{mq_properties = #mq_properties{regular_batch_of_msgs = BatchOfMsgs,
                                             regular_interval = Interval}} = State) ->
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#state{status = NextStatus,
                                         batch_of_msgs = BatchOfMsgs,
                                         interval = Interval}};

idling(#event_info{event = ?EVENT_DECR},
       #state{mq_properties = MQProps,
              batch_of_msgs = BatchOfMsgs,
              interval = Interval} = State) ->
    #mq_properties{max_interval = MaxInterval} = MQProps,
    {ok, {StepBatchOfMsgs, StepInterval}} = ?step_comsumption_values(MQProps),
    BatchOfMsgs_1 = decr_batch_procs_fun(BatchOfMsgs, StepBatchOfMsgs),
    Interval_1 = incr_interval_fun(Interval, MaxInterval, StepInterval),
    {next_state, ?ST_IDLING, State#state{status = ?ST_IDLING,
                                         batch_of_msgs = BatchOfMsgs_1,
                                         interval = Interval_1}};
idling(_, State) ->
    {next_state, ?ST_IDLING, State#state{status = ?ST_IDLING}}.


%% @doc State of 'running'
-spec(running(EventInfo, State) ->
             {next_state, ?ST_RUNNING, State, non_neg_integer()}
                 when EventInfo::#event_info{},
                      State::#state{}).
running(#event_info{event = ?EVENT_RUN,
                    is_force_exec = IsForceExec}, #state{id = Id,
                                                         publisher_id = PublisherId,
                                                         batch_of_msgs = BatchOfMsgs,
                                                         interval = Interval} = State) ->
    {NextStatus, State_2} =
        case catch consume(State, IsForceExec) of
            %% Execute the data-compaction repeatedly
            ok ->
                Interval_1 = case Interval of
                                 0 ->
                                     ?DEF_CONSUME_MIN_INTERVAL;
                                 _ ->
                                     Interval
                             end,
                timer:apply_after(Interval_1, ?MODULE, run, [Id]),
                {?ST_RUNNING, State};
            %% Reached end of the object-container
            not_found ->
                {_,State_1} = after_execute(ok, State),
                {?ST_IDLING, State_1};
            %% An unxepected error has occured
            {'EXIT', Cause} ->
                {_,State_1} = after_execute({error, Cause}, State),
                {?ST_IDLING, State_1};
            {error, short_interval = Cause} ->
                {_,State_1} = after_execute({error, Cause}, State),
                {?ST_RUNNING, State_1};
            %% An epected error has occured
            {error, Cause} ->
                {_,State_1} = after_execute({error, Cause}, State),
                {?ST_IDLING, State_1}
        end,

    ok = leo_mq_server:update_consumer_stats(
           PublisherId, NextStatus, BatchOfMsgs, Interval),
    {next_state, NextStatus, State_2#state{status = NextStatus,
                                           prev_proc_time = leo_date:clock()}};

running(#event_info{event = ?EVENT_SUSPEND}, #state{publisher_id = PublisherId,
                                                    batch_of_msgs = BatchOfMsgs,
                                                    interval = Interval} = State) ->
    NextStatus = ?ST_SUSPENDING,
    ok = leo_mq_server:update_consumer_stats(PublisherId, NextStatus, BatchOfMsgs, Interval),
    {next_state, NextStatus, State#state{status = NextStatus}};


running(#event_info{event = ?EVENT_INCR},
        #state{publisher_id = PublisherId,
               mq_properties = MQProps,
               interval = Interval,
               batch_of_msgs = BatchOfMsgs} = State) ->
    %% Retrieving the new interval and # of batch msgs
    #mq_properties{max_batch_of_msgs = MaxBatchOfMsgs} = MQProps,
    {ok, {StepBatchOfMsgs, StepInterval}} = ?step_comsumption_values(MQProps),
    BatchOfMsgs_1 =
        incr_batch_procs_fun(BatchOfMsgs, MaxBatchOfMsgs, StepBatchOfMsgs),
    Interval_1 = decr_interval_fun(Interval, StepInterval),

    %% Modify the items
    NextStatus = ?ST_RUNNING,
    ok = leo_mq_server:update_consumer_stats(
           PublisherId, NextStatus, BatchOfMsgs_1, Interval_1),
    {next_state, NextStatus, State#state{status = NextStatus,
                                         batch_of_msgs = BatchOfMsgs_1,
                                         interval = Interval_1}};

running(#event_info{event = ?EVENT_DECR},
        #state{publisher_id = PublisherId,
               mq_properties = MQProps,
               batch_of_msgs = BatchOfMsgs,
               interval = Interval} = State) ->
    %% Modify the interval
    #mq_properties{max_interval = MaxInterval} = MQProps,
    {ok, {StepBatchOfMsgs, StepInterval}} = ?step_comsumption_values(MQProps),
    Interval_1 = incr_interval_fun(Interval, MaxInterval, StepInterval),

    %% Modify the items
    {NextStatus, BatchOfMsgs_1} =
        case (BatchOfMsgs =< 0) of
            true ->
                {?ST_SUSPENDING, 0};
            false ->
                {?ST_RUNNING,
                 decr_batch_procs_fun(BatchOfMsgs, StepBatchOfMsgs)}
        end,

    ok = leo_mq_server:update_consumer_stats(
           PublisherId, NextStatus, BatchOfMsgs_1, Interval_1),
    {next_state, NextStatus, State#state{batch_of_msgs = BatchOfMsgs_1,
                                         interval = Interval_1,
                                         status = NextStatus}};

running(_, State) ->
    {next_state, ?ST_RUNNING, State#state{status = ?ST_RUNNING}}.

-spec(running( _, _, #state{}) ->
             {next_state, ?ST_RUNNING, #state{}, non_neg_integer()}).
running(#event_info{event = ?EVENT_STATE}, From, #state{status = Status} = State) ->
    gen_fsm:reply(From, {ok, Status}),
    {next_state, ?ST_RUNNING, State#state{status = ?ST_RUNNING}};
running(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    {next_state, ?ST_RUNNING, State#state{status = ?ST_RUNNING}}.


%% @doc State of 'suspend'
%%
-spec(suspending(EventInfo, State) ->
             {next_state, ?ST_SUSPENDING, State, non_neg_integer()}
                 when EventInfo::#event_info{},
                      State::#state{}).
suspending(#event_info{event = ?EVENT_RUN}, State) ->
    {next_state, ?ST_SUSPENDING, State#state{status = ?ST_SUSPENDING}};

suspending(#event_info{event = ?EVENT_STATE}, State) ->
    {next_state, ?ST_SUSPENDING, State#state{status = ?ST_SUSPENDING}};

suspending(#event_info{event = ?EVENT_INCR},
           #state{id = Id,
                  publisher_id = PublisherId,
                  mq_properties = MQProps,
                  batch_of_msgs = BatchOfMsgs,
                  interval = Interval} = State) ->
    %% Modify the item
    #mq_properties{max_batch_of_msgs = MaxBatchOfMsgs} = MQProps,
    {ok, {StepBatchOfMsgs, StepInterval}} = ?step_comsumption_values(MQProps),
    BatchOfMsgs_1 = incr_batch_procs_fun(BatchOfMsgs, MaxBatchOfMsgs, StepBatchOfMsgs),
    Interval_1 = decr_interval_fun(Interval, StepInterval),

    %% To the next status
    timer:apply_after(timer:seconds(1), ?MODULE, run, [Id]),
    NextStatus = ?ST_RUNNING,
    ok = leo_mq_server:update_consumer_stats(
           PublisherId, NextStatus, BatchOfMsgs_1, Interval_1),
    {next_state, NextStatus, State#state{status = NextStatus,
                                         batch_of_msgs = BatchOfMsgs_1,
                                         interval = Interval_1}};
suspending(_, State) ->
    {next_state, ?ST_SUSPENDING, State#state{status = ?ST_SUSPENDING}}.

-spec(suspending(EventInfo, From, State) ->
             {next_state, ?ST_SUSPENDING | ?ST_RUNNING, State, non_neg_integer()}
                 when EventInfo::#event_info{},
                      From::{pid(),Tag::atom()},
                      State::#state{}).
suspending(#event_info{event = ?EVENT_RESUME}, From, #state{id = Id,
                                                            publisher_id = PublisherId,
                                                            batch_of_msgs = BatchOfMsgs,
                                                            interval = Interval} = State) ->
    gen_fsm:reply(From, ok),
    ok = run(Id),
    ok = leo_mq_server:update_consumer_stats(PublisherId, ?ST_RUNNING, BatchOfMsgs, Interval),
    {next_state, ?ST_RUNNING, State#state{status = ?ST_RUNNING}};
suspending(#event_info{event = ?EVENT_STATE}, From, #state{status = Status} = State) ->
    gen_fsm:reply(From, {ok, Status}),
    {next_state, ?ST_SUSPENDING, State#state{status = ?ST_SUSPENDING}};
suspending(_Other, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    {next_state, ?ST_SUSPENDING, State#state{status = ?ST_SUSPENDING}}.


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
-spec(consume(State, IsForceExec) ->
             ok | not_found | {error, any()} when State::#state{},
                                                  IsForceExec::boolean()).
consume(#state{mq_properties = #mq_properties{
                                  db_procs = NumOfProcs,
                                  publisher_id = PublisherId,
                                  mod_callback = Mod},
               batch_of_msgs  = NumOfBatchProcs,
               prev_proc_time = PrevProcTime} = _State, IsForceExec) ->
    ThisTime = leo_date:clock(),
    Diff = erlang:round((ThisTime - PrevProcTime) / 1000),

    case (Diff >= ?DEF_CONSUME_MIN_INTERVAL
          orelse IsForceExec) of
        true ->
            NumOfBatchProcs_1 = leo_math:ceiling(NumOfBatchProcs / NumOfProcs),
            consume(PublisherId, Mod, NumOfBatchProcs_1);
        false ->
            {error, short_interval}
    end.

%% @doc Consume a message
%% @private
-spec(consume(atom(), atom(), non_neg_integer()) ->
             ok | not_found | {error, any()}).
consume(_Id,_,0) ->
    ok;
consume(Id, Mod, NumOfBatchProcs) ->
    case leo_mq_server:dequeue(Id) of
        {ok, MsgBin} ->
            try
                erlang:apply(Mod, handle_call, [{consume, Id, MsgBin}])
            catch
                _:Reason ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING},
                                            {function, "consume/3"},
                                            {line, ?LINE}, {body, [{module, Mod},
                                                                   {id, Id},
                                                                   {cause, Reason}
                                                                  ]}])
            after
                consume(Id, Mod, NumOfBatchProcs - 1)
            end;
        Other ->
            Other
    end.


%% @doc Defer a cosuming message
%%
-spec(defer_consume(atom(), pos_integer(), integer()) ->
             {ok, timer:tref()} | {error,_}).
defer_consume(Id, MaxInterval, MinInterval) ->
    defer_consume(Id, MaxInterval, MinInterval, false).

-spec(defer_consume(atom(), pos_integer(), integer(), boolean()) ->
             {ok, timer:tref()} | {error,_}).
defer_consume(Id, MaxInterval, MinInterval,_FromHandleInfo) ->
    Time = interval(Id, MinInterval, MaxInterval),
    timer:apply_after(Time, ?MODULE, run, [Id]).


%% @doc Retrieve interval of the waiting proc
%% @private
-spec(interval(Id, Interval, MaxInterval) ->
             Interval when Id::atom(),
                           Interval::non_neg_integer(),
                           MaxInterval::non_neg_integer()).
interval(Id, Interval, MaxInterval) when Interval < MaxInterval ->
    Interval_1 = erlang:phash2(Id, MaxInterval),
    Interval_2 = erlang:round((Interval_1 + random:uniform(MaxInterval))/2),
    Interval_3 = case (Interval_2 < Interval) of
                     true ->
                         Interval + erlang:phash2(Id, Interval);
                     false ->
                         Interval_2
                 end,
    Interval_3;
interval(_,Interval,_) ->
    Interval.


%% @doc Decrease the waiting time
%% @private
-spec(incr_interval_fun(Interval, MaxInterval, StepInterval) ->
             NewInterval when Interval::non_neg_integer(),
                              MaxInterval::non_neg_integer(),
                              StepInterval::non_neg_integer(),
                              NewInterval::non_neg_integer()).
incr_interval_fun(Interval, MaxInterval, StepInterval) ->
    erlang:min((Interval + StepInterval), MaxInterval).


%% @doc Decrease the waiting time
%% @private
-spec(decr_interval_fun(Interval, StepInterval) ->
             NewInterval when Interval::non_neg_integer(),
                              StepInterval::non_neg_integer(),
                              NewInterval::non_neg_integer()).
decr_interval_fun(Interval, StepInterval) ->
    erlang:max((Interval - StepInterval), ?DEF_CONSUME_MIN_INTERVAL).


%% @doc Increase the num of messages/batch-proccessing
%% @private
-spec(incr_batch_procs_fun(BatchProcs, MaxBatchProcs, StepBatchProcs) ->
             NewBatchProcs when BatchProcs::non_neg_integer(),
                                MaxBatchProcs::non_neg_integer(),
                                StepBatchProcs::non_neg_integer(),
                                NewBatchProcs::non_neg_integer()).
incr_batch_procs_fun(BatchProcs, MaxBatchProcs, StepBatchProcs) ->
    erlang:min((BatchProcs + StepBatchProcs), MaxBatchProcs).


%% @doc decrease the num of messages/batch-proccessing
%% @private
-spec(decr_batch_procs_fun(BatchProcs, StepBatchProcs) ->
             NewBatchProcs when BatchProcs::non_neg_integer(),
                                StepBatchProcs::non_neg_integer(),
                                NewBatchProcs::non_neg_integer()).
decr_batch_procs_fun(BatchProcs, StepBatchProcs) ->
    erlang:max((BatchProcs - StepBatchProcs), 0).
