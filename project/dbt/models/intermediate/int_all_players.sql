{{ config(materialized='view') }}

with all_players as (
    select 
        player_id,
        season,
        player_name,
        team_code,
        'G' as position,
        null as games_played,
        null as points_per_game,
        null as shooting_percentage
    from {{ ref('stg_goalies') }}
    
    union all
    
    select 
        player_id,
        season,
        player_name,
        team_code,
        position,
        games_played,
        points_per_game,
        shooting_percentage
    from {{ ref('stg_skaters') }}
)

select *,
    case 
        when position = 'G' then 'Goalie'
        when position in ('C', 'L', 'R', 'LW', 'RW') then 'Forward'
        when position in ('D') then 'Defense'
        else 'Unknown'
    end as position_group,
    current_timestamp as loaded_at
from all_players
