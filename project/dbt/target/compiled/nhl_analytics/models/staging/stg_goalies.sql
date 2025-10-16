

select
    playerId as player_id,
    season,
    name as player_name,
    team as team_code,
    position
from "nhl"."bronze"."goalies"
where playerId is not null
    and season is not null
    limit 10