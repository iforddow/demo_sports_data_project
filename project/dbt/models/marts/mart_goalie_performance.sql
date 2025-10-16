{{ config(
    materialized = 'table',
    post_hook = "{{ export_to_silver('mart_goalie_performance') }}"
) }}

select 
    player_id,
    player_name,
    season,
    team_code,
    position,
    loaded_at
from {{ ref('int_all_players') }}
where position_group in ('Goalie')
    and games_played >= 10
