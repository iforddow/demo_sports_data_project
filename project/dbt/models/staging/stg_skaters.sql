{{ config(materialized = 'view') }}
select playerId as player_id,
    season,
    name as player_name,
    team as team_code,
    position,
    sum(games_played) as games_played,
    sum(icetime) as ice_time,
    sum(shifts) as shifts,
    sum(gameScore) as game_score,
    sum(I_F_goals) as goals,
    sum(I_F_points) as points,
    sum(I_F_shotsOnGoal) as shots_on_goal,
    sum(I_F_primaryAssists) as primary_assists,
    sum(I_F_secondaryAssists) as secondary_assists,
    sum(I_F_xOnGoal) as expected_on_goal,
    sum(I_F_xGoals) as expected_goals,
    sum(I_F_xRebounds) as expected_rebounds,
    sum(I_F_rebounds) as rebounds,
    sum(I_F_freeze) as freeze,
    sum(I_F_playStopped) as play_stopped,
    sum(I_F_playContinuedInZone) as play_continued_in_zone,
    sum(I_F_playContinuedOutsideZone) as play_continued_outside_zone,
    sum(I_F_lowDangerShots) as low_danger_shots,
    sum(I_F_mediumDangerShots) as medium_danger_shots,
    sum(I_F_highDangerShots) as high_danger_shots,
    sum(I_F_lowDangerxGoals) as low_danger_expected_goals,
    sum(I_F_mediumDangerxGoals) as medium_danger_expected_goals,
    sum(I_F_highDangerxGoals) as high_danger_expected_goals,
    sum(I_F_lowDangerGoals) as low_danger_goals,
    sum(I_F_mediumDangerGoals) as medium_danger_goals,
    sum(I_F_highDangerGoals) as high_danger_goals,
    case
        when sum(games_played) > 0 then true
        else false
    end as is_active_player,
    case
        when sum(games_played) > 0 then round(sum(I_F_points) * 1.0 / sum(games_played), 2)
        else null
    end as points_per_game,
    case
        when sum(I_F_shotsOnGoal) > 0 then round(sum(I_F_goals) * 100.0 / sum(I_F_shotsOnGoal), 2)
        else null
    end as shooting_percentage,
    current_timestamp as loaded_at
from {{ source('bronze', 'skaters') }}
where playerId is not null
    and season is not null
group by playerId,
    season,
    name,
    team,
    position