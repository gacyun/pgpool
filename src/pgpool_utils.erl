-module(pgpool_utils).
-export([parse_db_url/1]).


parse_db_url(Url) ->
    [Schema, User, Pass, Host, Path] = string:tokens(Url, ":/@"),
    [DB | Options] = string:tokens(Path, "?"),
    Opts = case Options of
        [] -> [];
        [Options1] -> parse_db_options(string:tokens(Options1, "&"), [])
    end,
    {list_to_atom(Schema), env_filter(Host), env_filter(User), env_filter(Pass), [{database, env_filter(DB)}|Opts]}.


% private
env_filter(Param) ->
    case re:run(Param, "{{\s{0,}(.*[^\s])\s{0,}}}", [{capture, [1], list}]) of
        nomatch -> Param;
        {match,[EnvKey]} ->
            case os:getenv(string:to_upper(EnvKey)) of
                false -> "undefined";
                Value -> Value
            end
    end.

parse_db_options([], Result) -> Result;
parse_db_options([Option|Options], Result) ->
    case string:tokens(Option, "=") of
        [] -> [];
        [K] ->
            parse_db_options(Options, [{list_to_atom(K), undefined}|Result]);
        [K, V] ->
            parse_db_options(Options, [{list_to_atom(K), V}|Result])
    end.
