{{ config(materialized='view') }}

select
    playerId as player_id,
    season,
    name as player_name,
    team as team_code,
    position,
    games_played,
    icetime as ice_time,
    I_F_goals as goals,
    I_F_points as points,
    I_F_shotsOnGoal as shots_on_goal,
    I_F_primaryAssists as primary_assists,
    I_F_secondaryAssists as secondary_assists,
    case 
        when games_played > 0 then true 
        else false 
    end as is_active_player,
    case 
        when games_played > 0 then round(I_F_points * 1.0 / games_played, 2)
        else null 
    end as points_per_game,
    case 
        when I_F_shotsOnGoal > 0 then round(I_F_goals * 100.0 / I_F_shotsOnGoal, 2)
        else null 
    end as shooting_percentage,
    current_timestamp as loaded_at
from {{ source('bronze', 'skaters') }}
where playerId is not null
    and season is not null
