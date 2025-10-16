
  
    
    

    create  table
      "nhl"."silver"."mart_goalie_performance__dbt_tmp"
  
    as (
      

select 
    player_id,
    player_name,
    season,
    team_code,
    position,
    loaded_at
from "nhl"."silver"."int_all_players"
where position_group in ('Goalie')
    and games_played >= 10
    );
  
  