
  
  create view "nhl"."silver"."stg_teams__dbt_tmp" as (
    

select
    season,
    name as team_name,
    team as team_code,
    games_played,
    goalsFor,
    goalsAgainst,
    goalsFor - goalsAgainst as goal_differential,
    xGoalsFor,
    xGoalsAgainst,
    current_timestamp as loaded_at
from "nhl"."bronze"."teams"
where season is not null
    and name is not null
    and situation = 'all'
  );
