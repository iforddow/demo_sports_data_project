

select 
    playerId as player_id,
    season,
    name as player_name,
    team as team_code,
    position,
    max(games_played) as games_played,
    max(icetime) as ice_time,
    sum(xGoals) as expected_goals,
    sum(goals) as goals,
    sum(unblocked_shot_attempts) as unblocked_shot_attempts,
    sum(xRebounds) as expected_rebounds,
    sum(rebounds) as rebounds,
    sum(xFreeze) as expected_freeze,
    sum("freeze") as freeze,
    sum(xOnGoal) as expected_on_goal,
    sum(ongoal) as on_goal,
    sum(xPlayStopped) as expected_play_stopped,
    sum(playStopped) as play_stopped,
    sum(xPlayContinuedInZone) as expected_play_continued_in_zone,
    sum(playContinuedInZone) as play_continued_in_zone,
    sum(xPlayContinuedOutsideZone) as expected_play_continued_outside_zone,
    sum(playContinuedOutsideZone) as play_continued_outside_zone,
    sum(flurryAdjustedxGoals) as flurry_adjusted_expected_goals,
    sum(lowDangerShots) as low_danger_shots,
    sum(mediumDangerShots) as medium_danger_shots,
    sum(highDangerShots) as high_danger_shots,
    sum(lowDangerxGoals) as low_danger_expected_goals,
    sum(mediumDangerxGoals) as medium_danger_expected_goals,
    sum(highDangerxGoals) as high_danger_expected_goals,
    sum(lowDangerGoals) as low_danger_goals,
    sum(mediumDangerGoals) as medium_danger_goals,
    sum(highDangerGoals) as high_danger_goals,
    sum(blocked_shot_attempts) as blocked_shot_attempts,
    max(penalityMinutes) as penalty_minutes,
    max(penalties) as penalties,
    current_timestamp as loaded_at
from "nhl"."bronze"."goalies"
where playerId is not null
    and season is not null
group by playerId, season, name, team, position