{{ config(materialized='table') }}

select 
    player_id,
    player_name,
    team_code,
    position,
    position_group,
    games_played,
    points_per_game,
    shooting_percentage,
    case 
        when points_per_game >= 1.0 then 'Elite'
        when points_per_game >= 0.7 then 'Very Good'
        when points_per_game >= 0.5 then 'Good'
        when points_per_game >= 0.3 then 'Average'
        else 'Below Average'
    end as offensive_rating,
    loaded_at
from {{ ref('int_all_players') }}
where position_group in ('Forward', 'Defense')
    and games_played >= 10
