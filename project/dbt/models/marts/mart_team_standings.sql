{{ config(
    materialized = 'table',
    post_hook = "{{ export_to_silver('mart_team_standings') }}"
) }}

select 
    season,
    team_name,
    team_code,
    games_played,
    goalsFor,
    goalsAgainst,
    goal_differential,
    xGoalsFor,
    xGoalsAgainst,
    row_number() over (
        order by goalsFor desc,
            goal_differential desc
    ) as standings_rank,
    loaded_at
from {{ ref('stg_teams') }}
order by standings_rank
