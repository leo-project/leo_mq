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
%% Leo MQ - Server
%% @doc
%% @end
%%======================================================================
-module(leo_mq_server).

-author('Yosuke Hara').
-vsn('0.9.1').

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

-export([publish/3, consume/2, status/1]).

-ifdef(TEST).
-define(CURRENT_TIME, 65432100000).
-else.
-define(CURRENT_TIME, leo_utils:now()).
-endif.

-define(DEF_DB_PATH_INDEX,   "index"  ).
-define(DEF_DB_PATH_MESSAGE, "message").

-define(CONSUME_FORCE,   'force').
-define(CONSUME_REGULAR, 'regular').
-type(consume_type() :: ?CONSUME_FORCE | ?CONSUME_REGULAR).

-record(state, {id                 :: atom(),
                module             :: atom(),
                function           :: atom(),
                max_interval       :: integer(),
                min_interval       :: integer(),
                backend_index      :: atom(),
                backend_message    :: atom(),
                is_consume = false :: boolean()}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
start_link(Id, Props) ->
    gen_server:start_link({local, Id}, ?MODULE, [Id, Props], []).


stop(Id) ->
    gen_server:call(Id, stop).


%% @doc register queuing data.
%%
-spec(publish(atom(), binary(), binary()) -> ok | {error, any()}).
publish(Id, KeyBin, MessageBin) ->
    gen_server:cast(Id, {publish, KeyBin, MessageBin}).


%% @doc consume a message from the queue.
%%
-spec(consume(atom(), consume_type()) -> ok | {error, any()}).
consume(Id, Type) ->
    gen_server:cast(Id, {consume, Type}).


%% @doc get state from the queue.
%%
-spec(status(atom()) ->
             {ok, list()}).
status(Id) ->
    gen_server:call(Id, {status}).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([Id, #mq_properties{module       = Mod,
                         function     = Fun,
                         db_name      = DBName,
                         db_procs     = DBProcs,
                         root_path    = RootPath,
                         max_interval = MaxInterval,
                         min_interval = MinInterval}]) ->
    [{MQDBIndexPath,   MQDBIndexId},
     {MQDBMessagePath, MQDBMessageId}] = backend_db_info(Id, RootPath),

    Res0 = leo_mq_backend_db:start(MQDBIndexId,   DBProcs, DBName, MQDBIndexPath),
    Res1 = leo_mq_backend_db:start(MQDBMessageId, DBProcs, DBName, MQDBMessagePath),

    %% Id = ets:new(Id, [named_table, public, {read_concurrency, true}]),

    case (Res0 == ok andalso Res1 == ok) of
        true ->
            defer_consume(Id, ?CONSUME_REGULAR, MaxInterval, MinInterval),
            {ok, #state{id              = Id,
                        module          = Mod,
                        function        = Fun,
                        max_interval    = MaxInterval,
                        min_interval    = MinInterval,
                        backend_index   = MQDBIndexId,
                        backend_message = MQDBMessageId}};
        false ->
            {stop, 'filure backend_db launch'}
    end.

handle_call({status}, _From, #state{backend_index   = MQDBIndexId,
                                    backend_message = MQDBMessageId} = State) ->
    Res0 = leo_mq_backend_db:status(MQDBIndexId),
    Res1 = leo_mq_backend_db:status(MQDBMessageId),

    Count0 = lists:foldl(fun([{key_count, KC0}, _], Acc0) -> Acc0 + KC0;
                            (_, Acc0) -> Acc0
                         end, 0, Res0),
    Count1 = lists:foldl(fun([{key_count, KC1}, _], Acc1) -> Acc1 + KC1;
                            (_, Acc1) -> Acc1
                         end, 0, Res1),
    {reply, {ok, {Count0, Count1}}, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.


%% @doc Publish - Msg:"REPLICATE DATA".
%%
handle_cast({publish, KeyBin, MessageBin}, State) ->
    catch put_message(KeyBin, {leo_utils:clock(), MessageBin}, State),

    NewState = maybe_consume(State),
    {noreply, NewState};

%% @doc Consume a message from the queue.
%%
handle_cast({consume, ?CONSUME_REGULAR}, State) ->
    case catch maybe_consume(State) of
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "handle_cast/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {noreply, State};
        NewState ->
            {noreply, NewState}
    end;

handle_cast({consume, ?CONSUME_FORCE}, #state{id       = Id,
                                              module   = Mod,
                                              function = Fun,
                                              max_interval    = MaxInterval,
                                              min_interval    = MinInterval,
                                              backend_index   = BackendIndex,
                                              backend_message = BackendMessage} = State) ->
    case consume_fun(Id, Mod, Fun, BackendIndex, BackendMessage) of
        not_found ->
            {noreply, State#state{is_consume = false}};
        _Other ->
            defer_consume(Id, ?CONSUME_FORCE, MaxInterval, MinInterval),
            {noreply, State#state{is_consume = true}}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.


%% Function: handle_info(Info, State) -> {noreply, State}          |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%% handle_info({_Label, {_From, MRef}, get_modules}, State) ->
%%     {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.


%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
terminate(_Reason, _State) ->
    ok.


%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%--------------------------------------------------------------------
%%% INNER FUNCTIONS
%%--------------------------------------------------------------------
%% @doc maybe consume a message from the queue.
%%
-spec(maybe_consume(State::#state{}) ->
             #state{}).
maybe_consume(#state{is_consume = true} = State) ->
    State;
maybe_consume(#state{id = Id,
                     module   = Mod,
                     function = Fun,
                     max_interval    = MaxInterval,
                     min_interval    = MinInterval,
                     backend_index   = BackendIndex,
                     backend_message = BackendMessage,
                     is_consume      = false} = State) ->
    case consume_fun(Id, Mod, Fun, BackendIndex, BackendMessage) of
        not_found ->
            State#state{is_consume = false};
        _Other ->
            defer_consume(Id, ?CONSUME_FORCE, MaxInterval, MinInterval),
            State#state{is_consume = true}
    end.


%% @doc Consume a message
%%
-spec(consume_fun(atom(), atom(), atom(), atom(), atom()) ->
             ok | {error, any()}).
consume_fun(Id, Mod, Fun, BackendIndex, BackendMessage) ->
    try
        case leo_mq_backend_db:first(BackendIndex) of
            {ok, {K0, V0}} ->
                case leo_mq_backend_db:get(BackendMessage, V0) of
                    {ok, V1} ->
                        {_, MsgBin} = binary_to_term(V1),

                        catch erlang:apply(Mod, Fun, [Id, MsgBin]),
                        catch leo_mq_backend_db:delete(BackendIndex,   K0),
                        catch leo_mq_backend_db:delete(BackendMessage, V0),
                        ok;
                    not_found = Cause ->
                        {error, Cause};
                    Error ->
                        Error
                end;
            not_found = Cause->
                Cause;
            Error ->
                Error
        end
    catch
        _ : Why ->
            {error, Why}
    end.


%% @doc Defer a cosuming message
%%
-spec(defer_consume(atom(), consume_type(), integer(), integer()) ->
             ok).
defer_consume(Id, Type, MaxTime, MinTime) ->
    Time0 = random:uniform(MaxTime),
    Time1 = case (Time0 < MinTime) of
                true  -> MinTime;
                false -> Time0
            end,

    timer:apply_after(Time1, ?MODULE, consume, [Id, Type]).


%% @doc put a message into the queue.
%%
-spec(put_message(binary(), tuple(), #state{}) ->
             ok | {error, any()}).
put_message(MsgKeyBin, {MsgId, _MsgBin} = MsgTuple, #state{backend_index   = BackendIndex,
                                                           backend_message = BackendMessage}) ->
    MsgIdBin   = term_to_binary(MsgId),
    MessageBin = term_to_binary(MsgTuple),

    try
        case leo_mq_backend_db:get(BackendMessage, MsgKeyBin) of
            not_found ->
                case leo_mq_backend_db:put(BackendIndex, MsgIdBin, MsgKeyBin) of
                    ok ->
                        case leo_mq_backend_db:put(BackendMessage, MsgKeyBin, MessageBin) of
                            ok ->
                                ok;
                            Error ->
                                leo_mq_backend_db:delete(BackendIndex, MsgIdBin),
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

