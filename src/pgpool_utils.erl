-module(pgpool_utils).
-export([parse_db_url/1]).


parse_db_url(Url) ->
    [Schema, User, Pass, Host, Path] = string:tokens(Url, ":/@"),
    [DB | Options] = string:tokens(Path, "?"),
    Opts = case Options of
        [] -> [];
        [Options1] -> parse_db_options(string:tokens(Options1, "&"), [])
    end,
    {list_to_atom(Schema), Host, User, Pass, [{database, DB}|Opts]}.


% private
parse_db_options([], Result) -> Result;
parse_db_options([Option|Options], Result) ->
    case string:tokens(Option, "=") of
        [] -> [];
        [K] ->
            parse_db_options(Options, [{list_to_atom(K), undefined}|Result]);
        [K, V] ->
            parse_db_options(Options, [{list_to_atom(K), V}|Result])
    end.
