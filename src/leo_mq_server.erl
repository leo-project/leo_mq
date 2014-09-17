%%======================================================================
%%
%% Leo MQ
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
%% ---------------------------------------------------------------------
%% Leo MQ - Server
%% @doc
%% @end
%%======================================================================
-module(leo_mq_server).

-author('Yosuke Hara').

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

-export([publish/3, consume/1, status/1, close/1]).

-ifdef(TEST).
-define(CURRENT_TIME, 65432100000).
-else.
-define(CURRENT_TIME, leo_date:now()).
-endif.

-define(DEF_DB_PATH_INDEX,   "index"  ).
-define(DEF_DB_PATH_MESSAGE, "message").

-define(DEF_TIMEOUT, 30000).
-define(DEF_AFTER_NOT_FOUND_INTERVAL_MIN,  5000).
-define(DEF_AFTER_NOT_FOUND_INTERVAL_MAX, 10000).

-record(state, {id               :: atom(),
                module           :: atom(),
                max_interval = 1 :: integer(),
                min_interval = 1 :: integer(),
                backend_index    :: atom(),
                backend_message  :: atom(),
                num_of_batch_processes = 1 :: pos_integer()
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
    gen_server:cast(Id, {publish, KeyBin, MessageBin}).


%% @doc Consume a message from the queue.
%%
-spec(consume(Id) ->
             ok | {error, any()} when Id::atom()).
consume(Id) ->
    gen_server:cast(Id, consume).


%% @doc Retrieve the current state from the queue.
%%
-spec(status(Id) ->
             {ok, list()} when Id::atom()).
status(Id) ->
    gen_server:call(Id, status, ?DEF_TIMEOUT).


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
init([Id, #mq_properties{module                 = Mod,
                         db_name                = DBName,
                         db_procs               = DBProcs,
                         root_path              = RootPath,
                         num_of_batch_processes = NumOfBatchProc,
                         max_interval           = MaxInterval,
                         min_interval           = MinInterval}]) ->
    [{MQDBIndexPath,   MQDBIndexId},
     {MQDBMessagePath, MQDBMessageId}] = backend_db_info(Id, RootPath),

    case application:get_env(leo_mq, backend_db_sup_ref) of
        {ok, Pid} ->
            Res0 = leo_backend_db_sup:start_child(Pid, MQDBIndexId,
                                                  DBProcs, DBName, MQDBIndexPath),
            Res1 = leo_backend_db_sup:start_child(Pid, MQDBMessageId,
                                                  DBProcs, DBName, MQDBMessagePath),
            case (Res0 == ok andalso Res1 == ok) of
                true ->
                    defer_consume(Id, MaxInterval, MinInterval),
                    {ok, #state{id                     = Id,
                                module                 = Mod,
                                num_of_batch_processes = NumOfBatchProc,
                                max_interval           = MaxInterval,
                                min_interval           = MinInterval,
                                backend_index          = MQDBIndexId,
                                backend_message        = MQDBMessageId}};
                false ->
                    {stop, "Failure backend_db launch"}
            end;
        _Error ->
            {stop, "Not initialized"}
    end.


%% @doc gen_server callback - Module:handle_call(Request, From, State) -> Result
handle_call(status, _From, #state{backend_index   = MQDBIndexId,
                                  backend_message = MQDBMessageId} = State) ->
    Res0 = leo_backend_db_api:status(MQDBIndexId),
    Res1 = leo_backend_db_api:status(MQDBMessageId),

    Count0 = lists:foldl(fun([{key_count, KC0}, _], Acc0) -> Acc0 + KC0;
                            (_, Acc0) -> Acc0
                         end, 0, Res0),
    Count1 = lists:foldl(fun([{key_count, KC1}, _], Acc1) -> Acc1 + KC1;
                            (_, Acc1) -> Acc1
                         end, 0, Res1),
    {reply, {ok, {Count0, Count1}}, State};

handle_call(close, _From, #state{backend_index   = MQDBIndexId,
                                 backend_message = MQDBMessageId} = State) ->
    ok = close_db(MQDBIndexId),
    ok = close_db(MQDBMessageId),
    {reply, ok, State};

handle_call(stop, _From, State) ->
    {stop, shutdown, ok, State}.


%% @doc gen_server callback - Module:handle_cast(Request, State) -> Result
handle_cast({publish, KeyBin, MessageBin}, State = #state{id     = Id,
                                                          module = Mod}) ->
    Reply = put_message(KeyBin, {leo_date:clock(), MessageBin}, State),
    catch erlang:apply(Mod, handle_call, [{publish, Id, Reply}]),

    {noreply, State};


handle_cast(consume, #state{id                     = Id,
                            module                 = Mod,
                            num_of_batch_processes = NumOfBatchProc,
                            max_interval           = MaxInterval,
                            min_interval           = MinInterval,
                            backend_index          = BackendIndex,
                            backend_message        = BackendMessage} = State) ->
    case consume_fun(Id, Mod, BackendIndex, BackendMessage, NumOfBatchProc) of
        not_found ->
            defer_consume(Id, ?DEF_AFTER_NOT_FOUND_INTERVAL_MAX,
                          ?DEF_AFTER_NOT_FOUND_INTERVAL_MIN),
            {noreply, State};
        _Other ->
            defer_consume(Id, MaxInterval, MinInterval),
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.


%% @doc gen_server callback - Module:handle_info(Info, State) -> Result
handle_info(_Info, State) ->
    {noreply, State}.


%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% <p>
%% gen_server callback - Module:terminate(Reason, State)
%% </p>
terminate(_Reason, #state{id = Id}) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "terminate/1"},
                           {line, ?LINE}, {body, Id}]),
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
%% @doc Consume a message
%%
-spec(consume_fun(atom(), atom(), atom(), atom(), pos_integer()) ->
             ok | not_found | {error, any()}).
consume_fun(Id, Mod, BackendIndex, BackendMessage, NumOfBatchProc) ->
    try
        case leo_backend_db_api:first(BackendIndex) of
            {ok, {K0, V0}} ->
                case leo_backend_db_api:get(BackendMessage, V0) of
                    {ok, V1} ->
                        Term = binary_to_term(V1),
                        {_, MsgBin} = Term,

                        erlang:apply(Mod, handle_call, [{consume, Id, MsgBin}]),
                        catch leo_backend_db_api:delete(BackendIndex,   K0),
                        catch leo_backend_db_api:delete(BackendMessage, V0),
                        consume_fun(Id, Mod, BackendIndex, BackendMessage, NumOfBatchProc - 1);
                    not_found = Cause ->
                        {error, Cause};
                    Error ->
                        Error
                end;
            not_found = Cause ->
                Cause;
            Error ->
                Error
        end
    catch
        _: Why ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "consume_fun/5"},
                                    {line, ?LINE}, {body, Why}]),
            {error, Why}
    end.


%% @doc Defer a cosuming message
%%
-spec(defer_consume(atom(), pos_integer(), integer()) ->
             {ok, timer:tref()} | {error,_}).
defer_consume(Id, MaxTime, MinTime) ->
    Time0 = random:uniform(MaxTime),
    Time1 = case (Time0 < MinTime) of
                true  -> MinTime;
                false -> Time0
            end,
    timer:apply_after(Time1, ?MODULE, consume, [Id]).


%% @doc put a message into the queue.
%%
-spec(put_message(binary(), tuple(), #state{}) ->
             ok | {error, any()}).
put_message(MsgKeyBin, {MsgId, _MsgBin} = MsgTuple, #state{backend_index   = BackendIndex,
                                                           backend_message = BackendMessage}) ->
    MsgIdBin   = term_to_binary(MsgId),
    MessageBin = term_to_binary(MsgTuple),

    try
        case leo_backend_db_api:get(BackendMessage, MsgKeyBin) of
            not_found ->
                case leo_backend_db_api:put(BackendIndex, MsgIdBin, MsgKeyBin) of
                    ok ->
                        case leo_backend_db_api:put(BackendMessage, MsgKeyBin, MessageBin) of
                            ok ->
                                ok;
                            Error ->
                                leo_backend_db_api:delete(BackendIndex, MsgIdBin),
                                Error
                        end;
                    Error ->
                        Error
                end;
            _Other ->
                ok
        end
    catch
        _ : Cause ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "put_message/3"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Retrieve backend-db information.
%%
-spec(backend_db_info(atom(), string()) ->
             list()).
backend_db_info(Id, RootPath) ->
    NewRootPath = case (string:len(RootPath) == string:rstr(RootPath, "/")) of
                      true  -> RootPath;
                      false ->  RootPath ++ "/"
                  end,

    MQDBIndexPath   = NewRootPath ++ ?DEF_DB_PATH_INDEX,
    MQDBMessagePath = NewRootPath ++ ?DEF_DB_PATH_MESSAGE,

    MQDBIndexId   = list_to_atom(atom_to_list(Id) ++ "_index"),
    MQDBMessageId = list_to_atom(atom_to_list(Id) ++ "_message"),

    [{MQDBIndexPath, MQDBIndexId},
     {MQDBMessagePath, MQDBMessageId}].


%% @doc Close a db
%% @private
-spec(close_db(atom()) ->
             ok).
close_db(InstanseName) ->
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
