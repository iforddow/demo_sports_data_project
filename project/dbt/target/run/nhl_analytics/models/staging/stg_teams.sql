
  
  create view "nhl"."silver"."stg_teams__dbt_tmp" as (
    
select season,
    name as team_name,
    team as team_code,
    sum(games_played) as games_played,
    sum(goalsFor) as goalsFor,
    sum(goalsAgainst) as goalsAgainst,
    sum(goalsFor) - sum(goalsAgainst) as goal_differential,
    sum(xGoalsFor) as xGoalsFor,
    sum(xGoalsAgainst) as xGoalsAgainst,
    current_timestamp as loaded_at
from "nhl"."bronze"."teams"
where season is not null
    and name is not null
group by season,
    name,
    team
  );
