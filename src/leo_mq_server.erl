%%======================================================================
%%
%% Leo MQ
%%
%% Copyright (c) 2012-2017 Rakuten, Inc.
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
%% @doc The gen_server process for the process of a mq's enqueue/dequeue as part of a supervision tree
%% @reference https://github.com/leo-project/leo_mq/blob/master/src/leo_mq_server.erl
%% @end
%%======================================================================
-module(leo_mq_server).
-behaviour(gen_server).

-include("leo_mq.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/3,
         stop/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([enqueue/3, dequeue/1, close/1]).
-export([peek/1, peek/2, remove/2, has_key/2]).

-ifdef(TEST).
-define(CURRENT_TIME, 65432100000).
-else.
-define(CURRENT_TIME, leo_date:now()).
-endif.

-define(DEF_TIMEOUT, timer:seconds(10)).
-define(DEF_AFTER_NOT_FOUND_INTERVAL_MIN,  5000).
-define(DEF_AFTER_NOT_FOUND_INTERVAL_MAX, 10000).

-record(state, {id :: atom(),
                seq_num = 0 :: non_neg_integer(),
                backend_db_id :: atom(),
                mq_properties = #mq_properties{} :: #mq_properties{},
                state_filepath = [] :: string()
               }).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Creates the gen_server process as part of a supervision tree
-spec(start_link(Id, WorkerSeqNum, Props) ->
             {ok,pid()} | ignore | {error, any()} when Id::atom(),
                                                       WorkerSeqNum::non_neg_integer(),
                                                       Props::[tuple()]).
start_link(Id, WorkerSeqNum, Props) ->
    gen_server:start_link({local, Id}, ?MODULE, [Id, WorkerSeqNum, Props], []).


%% @doc Close the process
-spec(stop(Id) ->
             ok | {error, any()} when Id::atom()).
stop(Id) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "stop/1"},
                           {line, ?LINE}, {body, Id}]),
    gen_server:call(Id, stop, ?DEF_TIMEOUT).


%% @doc Register a queuing data.
-spec(enqueue(Id, KeyBin, MessageBin) ->
             ok | {error, any()} when Id::atom(),
                                      KeyBin::binary(),
                                      MessageBin::binary()).
enqueue(Id, KeyBin, MessageBin) ->
    gen_server:call(Id, {enqueue, KeyBin, MessageBin}, ?DEF_TIMEOUT).


%% @doc Register a queuing data.
dequeue(Id) ->
    gen_server:call(Id, dequeue, ?DEF_TIMEOUT).


%% @doc Peek an item in a queuing data.
-spec(peek(Id) ->
             {ok, KeyBin, MessageBin} | {error, any()} | not_found
                                when Id::atom(),
                                     KeyBin::binary(),
                                     MessageBin::binary()).
peek(Id) ->
    gen_server:call(Id, peek, ?DEF_TIMEOUT).

%% @doc Peek the first N items in a queuing data.
-spec(peek(Id, N) ->
             {ok, list({KeyBin, MessageBin})} | {error, any()} | not_found
                                when Id::atom(),
                                     N::pos_integer(),
                                     KeyBin::binary(),
                                     MessageBin::binary()).
peek(Id, N) ->
    gen_server:call(Id, {peek, N}, ?DEF_TIMEOUT).


%% @doc Remove an item in a queuing data.
-spec(remove(Id, KeyBin) ->
             ok | {error, any()} when Id::atom(),
                                      KeyBin::binary()).
remove(Id, KeyBin) ->
    gen_server:call(Id, {remove, KeyBin}, ?DEF_TIMEOUT).


%% @doc Has the key of a message in the queue.
-spec(has_key(Id, KeyBin) ->
             boolean() | {error, any()} when Id::atom(),
                                             KeyBin::binary()).
has_key(Id, KeyBin) ->
    gen_server:call(Id, {has_key, KeyBin}, ?DEF_TIMEOUT).


%% @doc get state from the queue.
-spec(close(Id) ->
             ok when Id::atom()).
close(Id) ->
    gen_server:call(Id, close, ?DEF_TIMEOUT).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% @doc gen_server callback - Module:init(Args) -> Result
init([Id, WorkerSeqNum, #mq_properties{db_name = DBName,
                                       db_procs = DBProcs,
                                       mqdb_id = MQDBMessageId,
                                       mqdb_path = MQDBMessagePath} = MQProps]) ->
    case erlang:whereis(leo_backend_db_sup) of
        undefined ->
            {stop, 'not_initialized'};
        _ ->
            MQDBMessageId_1 =
                case (DBProcs > 1) of
                    true ->
                        list_to_atom(
                          lists:append([atom_to_list(MQDBMessageId), "_0"]));
                    false ->
                        MQDBMessageId
                end,
            case erlang:whereis(MQDBMessageId_1) of
                undefined ->
                    leo_backend_db_sup:start_child(
                      leo_backend_db_sup, MQDBMessageId,
                      DBProcs, DBName, MQDBMessagePath);
                _ ->
                    void
            end,

            IdStr = atom_to_list(Id),
            IdStr_1 = string:substr(IdStr, 1,
                                    string:rstr(IdStr, "_") - 1),
            SeqNum = WorkerSeqNum - 1,

            {ok, #state{id = Id,
                        seq_num = SeqNum,
                        backend_db_id = list_to_atom(
                                          lists:append([IdStr_1,
                                                        "_message_",
                                                        integer_to_list(SeqNum)
                                                       ])),
                        mq_properties = MQProps}, ?DEF_TIMEOUT}
    end.


%% @doc gen_server callback - Module:handle_call(Request, From, State) -> Result
handle_call({enqueue, KeyBin, MessageBin}, _From, #state{backend_db_id = BackendDbId} = State) ->
    Reply_1 =
        case leo_backend_db_server:get(BackendDbId, KeyBin) of
            not_found ->
                put_message(KeyBin, MessageBin, BackendDbId);
            _ ->
                ok
        end,
    {reply, Reply_1, State, ?DEF_TIMEOUT};

handle_call(dequeue, _From, #state{backend_db_id = BackendDbId} = State) ->
    Reply = case catch leo_backend_db_server:first(BackendDbId) of
                {ok, Key, Val} ->
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
                    case catch leo_backend_db_server:delete(BackendDbId, Key) of
                        ok ->
                            {ok, MsgBin};
                        {_, Why} ->
                            error_logger:error_msg("~p,~p,~p,~p~n",
                                                   [{module, ?MODULE_STRING},
                                                    {function, "handle_call/3"},
                                                    {line, ?LINE}, {body, Why}]),
                            {error, Why}
                    end;
                not_found = Cause ->
                    Cause;
                {_, Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING},
                                            {function, "handle_call/3"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause}
            end,
    {reply, Reply, State, ?DEF_TIMEOUT};

handle_call(peek, _From, #state{backend_db_id = BackendDbId} = State) ->
    Reply = case catch leo_backend_db_server:first(BackendDbId) of
                {ok, Key, Val} ->
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

                    {ok, Key, MsgBin};
                not_found = Cause ->
                    Cause;
                {_, Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING},
                                            {function, "handle_call/3"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause}
            end,
    {reply, Reply, State, ?DEF_TIMEOUT};

handle_call({peek, N}, _From, #state{backend_db_id = BackendDbId} = State) ->
    Reply = case catch leo_backend_db_server:first_n(BackendDbId, N) of
                {ok, List0} ->
                    Fun = fun({Key, Val}) ->
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
                                  {Key, MsgBin}
                          end,
                    List = lists:map(Fun, List0),
                    {ok, List};
                not_found = Cause ->
                    Cause;
                {_, Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING},
                                            {function, "handle_call/3"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause}
            end,
    {reply, Reply, State, ?DEF_TIMEOUT};

handle_call({remove, KeyBin}, _From, #state{backend_db_id = BackendDbId} = State) ->
    Reply = case catch leo_backend_db_server:delete(BackendDbId, KeyBin) of
                ok ->
                    ok;
                {_, Why} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING},
                                            {function, "handle_call/3"},
                                            {line, ?LINE}, {body, Why}]),
                    {error, Why}
            end,
    {reply, Reply, State, ?DEF_TIMEOUT};

handle_call({has_key, KeyBin}, _From, #state{backend_db_id = BackendDbId} = State) ->
    Reply = case catch leo_backend_db_server:get(BackendDbId, KeyBin) of
                {ok,_} ->
                    true;
                not_found ->
                    false;
                {_, Why} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING},
                                            {function, "handle_call/3"},
                                            {line, ?LINE}, {body, Why}]),
                    {error, Why}
            end,
    {reply, Reply, State, ?DEF_TIMEOUT};

%% handle_call(status, _From, #state{count = Count} = State) ->
%%     {reply, {ok, Count}, State, ?DEF_TIMEOUT};

handle_call(close, _From, #state{mq_properties = MQProps} = State) ->
    MQDBMessageId = MQProps#mq_properties.mqdb_id,
    ok = close_db(MQDBMessageId),
    {reply, ok, State, ?DEF_TIMEOUT};

handle_call(stop, _From, State) ->
    {stop, shutdown, ok, State}.


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
                          mq_properties = MQProps}) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "terminate/1"},
                           {line, ?LINE}, {body, Id}]),
    %% Close the backend_db
    MQDBMessageId = MQProps#mq_properties.mqdb_id,
    ok = close_db(MQDBMessageId),
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
-spec(put_message(MsgKeyBin, MsgBin, BackendDbId) ->
             ok | {error, any()} when MsgKeyBin::binary(),
                                      MsgBin::binary(),
                                      BackendDbId::atom()).
put_message(MsgKeyBin, MsgBin, BackendDbId) ->
    case catch leo_backend_db_server:put(
                 BackendDbId, MsgKeyBin, MsgBin) of
        ok ->
            ok;
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "put_message/3"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        Error ->
            Error
    end.


%% @doc Close a db
%% @private
-spec(close_db(InstanceName) ->
             ok when InstanceName::atom()).
close_db(InstanseName) ->
    %% Close the backend-db
    SupRef = leo_backend_db_sup,
    case erlang:whereis(SupRef) of
        undefined ->
            ok;
        _Pid ->
            List = supervisor:which_children(SupRef),
            ok = close_db(List, InstanseName),
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
