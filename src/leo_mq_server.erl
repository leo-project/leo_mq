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
%% Leo MQ - Server
%% @doc The gen_server process for the process of a mq's publisher as part of a supervision tree
%% @reference https://github.com/leo-project/leo_mq/blob/master/src/leo_mq_server.erl
%% @end
%%======================================================================
-module(leo_mq_server).

-behaviour(gen_server).

-include("leo_mq.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/2,
         stop/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([publish/3, dequeue/1,
         status/1, update_consumer_stats/4, close/1]).

-ifdef(TEST).
-define(CURRENT_TIME, 65432100000).
-else.
-define(CURRENT_TIME, leo_date:now()).
-endif.

-define(DEF_TIMEOUT, timer:seconds(10)).
-define(DEF_AFTER_NOT_FOUND_INTERVAL_MIN,  5000).
-define(DEF_AFTER_NOT_FOUND_INTERVAL_MAX, 10000).

-record(state, {id :: atom(),
                mq_properties = #mq_properties{} :: #mq_properties{},
                count = 0 :: non_neg_integer(),
                consumer_status = ?ST_IDLING :: state_of_mq(),
                consumer_batch_of_msgs = 0 :: non_neg_integer(),
                consumer_interval = 0 :: non_neg_integer(),
                state_filepath = [] :: string()
               }).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Creates the gen_server process as part of a supervision tree
-spec(start_link(Id, Props) ->
             {ok,pid()} | ignore | {error, any()} when Id::atom(),
                                                       Props::[tuple()]).
start_link(Id, Props) ->
    gen_server:start_link({local, Id}, ?MODULE, [Id, Props], []).

%% @doc Close the process
-spec(stop(Id) ->
             ok | {error, any()} when Id::atom()).
stop(Id) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "stop/1"},
                           {line, ?LINE}, {body, Id}]),
    gen_server:call(Id, stop, ?DEF_TIMEOUT).


%% @doc Register a queuing data.
%%
-spec(publish(Id, KeyBin, MessageBin) ->
             ok | {error, any()} when Id::atom(),
                                      KeyBin::binary(),
                                      MessageBin::binary()).
publish(Id, KeyBin, MessageBin) ->
    gen_server:call(Id, {publish, KeyBin, MessageBin}, ?DEF_TIMEOUT).


%% @doc Register a queuing data.
%%
dequeue(Id) ->
    gen_server:call(Id, dequeue, ?DEF_TIMEOUT).


%% @doc Retrieve the current state from the queue.
%%
-spec(status(Id) ->
             {ok, [{atom(), any()}]} when Id::atom()).
status(Id) ->
    gen_server:call(Id, status, ?DEF_TIMEOUT).


%% @doc Retrieve the current state from the queue.
%%
-spec(update_consumer_stats(Id, CnsStatus, CnsBatchOfMsgs, CnsIntervalBetweenBatchProcs) ->
             ok when Id::atom(),
                     CnsStatus::state_of_mq(),
                     CnsBatchOfMsgs::non_neg_integer(),
                     CnsIntervalBetweenBatchProcs::non_neg_integer()).
update_consumer_stats(Id, CnsStatus, CnsBatchOfMsgs, CnsIntervalBetweenBatchProcs) ->
    gen_server:cast(Id, {update_consumer_stats, CnsStatus, CnsBatchOfMsgs, CnsIntervalBetweenBatchProcs}).


%% @doc get state from the queue.
%%
-spec(close(Id) ->
             ok when Id::atom()).
close(Id) ->
    gen_server:call(Id, close, ?DEF_TIMEOUT).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% @doc gen_server callback - Module:init(Args) -> Result
init([Id, #mq_properties{db_name   = DBName,
                         db_procs  = DBProcs,
                         mqdb_id   = MQDBMessageId,
                         mqdb_path = MQDBMessagePath,
                         root_path = RootPath
                        } = MQProps]) ->
    case application:get_env(leo_mq, backend_db_sup_ref) of
        {ok, Pid} ->
            ok = leo_backend_db_sup:start_child(
                   Pid, MQDBMessageId,
                   DBProcs, DBName, MQDBMessagePath),
            %% Retrieve total num of message from the local state file
            StateFilePath = filename:join([RootPath, atom_to_list(Id)]),
            Count = case file:consult(StateFilePath) of
                        {ok, Props} ->
                            leo_misc:get_value('count', Props, 0);
                        _ ->
                            0
                    end,
            {ok, #state{id = Id,
                        mq_properties = MQProps,
                        count = Count,
                        state_filepath = StateFilePath}, ?DEF_TIMEOUT};
        _Error ->
            {stop, 'not_initialized'}
    end.


%% @doc gen_server callback - Module:handle_call(Request, From, State) -> Result
handle_call({publish, KeyBin, MessageBin}, _From, #state{count = Count} = State) ->
    Reply = put_message(KeyBin, MessageBin, State),
    {reply, Reply, State#state{count = Count + 1}, ?DEF_TIMEOUT};

handle_call(dequeue, _From, #state{count = Count,
                                   mq_properties = MQProps} = State) ->
    MQDBMessageId = MQProps#mq_properties.mqdb_id,
    {Reply, Count_1} =
        case catch leo_backend_db_api:first(MQDBMessageId) of
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

                %% Remove the queue from the backend-db
                case catch leo_backend_db_api:delete(MQDBMessageId, Key) of
                    ok when Count > 0 ->
                        {{ok, MsgBin}, Count - 1};
                    ok ->
                        {{ok, MsgBin}, Count};
                    {_, Why} ->
                        error_logger:error_msg("~p,~p,~p,~p~n",
                                               [{module, ?MODULE_STRING},
                                                {function, "handle_call/3"},
                                                {line, ?LINE}, {body, Why}]),
                        {{error, Why}, Count}
                end;
            not_found = Cause ->
                {Cause, 0};
            {_, Cause} ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING},
                                        {function, "handle_call/3"},
                                        {line, ?LINE}, {body, Cause}]),
                {{error, Cause}, Count}
        end,
    {reply, Reply, State#state{count = Count_1}, ?DEF_TIMEOUT};

handle_call(status, _From, #state{consumer_status = ConsumerStatus,
                                  consumer_batch_of_msgs = BatchOfMsgs,
                                  consumer_interval = Interval,
                                  count = Count} = State) ->
    {reply, {ok, [{?MQ_CNS_PROP_NUM_OF_MSGS, Count},
                  {?MQ_CNS_PROP_STATUS, ConsumerStatus},
                  {?MQ_CNS_PROP_BATCH_OF_MSGS, BatchOfMsgs},
                  {?MQ_CNS_PROP_INTERVAL, Interval}
                 ]}, State, ?DEF_TIMEOUT};

handle_call(close, _From, #state{count = Count,
                                 state_filepath = StateFilePath,
                                 mq_properties = MQProps} = State) ->
    MQDBMessageId = MQProps#mq_properties.mqdb_id,
    ok = close_db(MQDBMessageId, Count, StateFilePath),
    {reply, ok, State, ?DEF_TIMEOUT};

handle_call(stop, _From, State) ->
    {stop, shutdown, ok, State}.


%% @doc gen_server callback - Module:handle_cast(Request, State) -> Result
handle_cast({update_consumer_stats, CnsStatus, CnsBatchOfMsgs, CnsIntervalBetweenBatchProcs}, State) ->
    {noreply, State#state{consumer_status = CnsStatus,
                          consumer_batch_of_msgs = CnsBatchOfMsgs,
                          consumer_interval = CnsIntervalBetweenBatchProcs}, ?DEF_TIMEOUT};
handle_cast(_Msg, State) ->
    {noreply, State, ?DEF_TIMEOUT}.


%% @doc gen_server callback - Module:handle_info(Info, State) -> Result
handle_info(timeout, State) ->
    {noreply, State, ?DEF_TIMEOUT};
handle_info(_Info, State) ->
    {noreply, State, ?DEF_TIMEOUT}.


%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% <p>
%% gen_server callback - Module:terminate(Reason, State)
%% </p>
terminate(_Reason, #state{id = Id,
                          count = Count,
                          state_filepath = StateFilePath,
                          mq_properties = MQProps}) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "terminate/1"},
                           {line, ?LINE}, {body, Id}]),
    %% Close the backend_db
    MQDBMessageId = MQProps#mq_properties.mqdb_id,
    ok = close_db(MQDBMessageId, Count, StateFilePath),
    ok.


%% @doc Convert process state when code is changed
%% <p>
%% gen_server callback - Module:code_change(OldVsn, State, Extra) -> {ok, NewState} | {error, Reason}.
%% </p>
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%--------------------------------------------------------------------
%%% INNER FUNCTIONS
%%--------------------------------------------------------------------
%% @doc put a message into the queue.
%%
-spec(put_message(binary(), tuple(), #state{}) ->
             ok | {error, any()}).
put_message(MsgKeyBin, MsgBin, #state{mq_properties = MQProps}) ->
    try
        MQDBMessageId = MQProps#mq_properties.mqdb_id,
        case leo_backend_db_api:put(
               MQDBMessageId, MsgKeyBin, MsgBin) of
            ok ->
                ok;
            Error ->
                Error
        end
    catch
        _:Cause ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "put_message/3"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Close a db
%% @private
-spec(close_db(InstanceName, Count, StateFilePath) ->
             ok when InstanceName::atom(),
                     Count::non_neg_integer(),
                     StateFilePath::string()).
close_db(InstanseName, Count, StateFilePath) ->
    %% Output the current state
    catch leo_file:file_unconsult(StateFilePath, [{id, InstanseName},
                                                  {count, Count}
                                                 ]),
    %% Close the backend-db
    case whereis(leo_backend_db_sup) of
        Pid when is_pid(Pid) ->
            List = supervisor:which_children(Pid),
            ok = close_db(List, InstanseName),
            ok;
        _ ->
            ok
    end.

%% @private
close_db([],_) ->
    ok;
close_db([{Id,_Pid, worker, ['leo_backend_db_server' = Mod|_]}|T], InstanceName) ->
    case (string:str(atom_to_list(Id),
                     atom_to_list(InstanceName)) > 0) of
        true ->
            ok = Mod:close(Id);
        false ->
            void
    end,
    close_db(T, InstanceName);
close_db([_|T], InstanceName) ->
    close_db(T, InstanceName).
