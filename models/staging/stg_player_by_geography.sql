SELECT
  stg_player_locations.geography
, stg_player_locations.player_id
, SUM(tournament_count_rolling_12) AS tournament_count_rolling_12
, SUM(points_rolling_12) AS points_rolling_12
, SUM(position_perc_rolling_12) / SUM(tournament_count_rolling_12) AS position_perc_rolling_12
, SUM(points_perc_rolling_12) / SUM(tournament_count_rolling_12) AS points_perc_rolling_12
, SUM(tournament_count_rolling_48) AS tournament_count_rolling_48
, SUM(points_rolling_48) AS points_rolling_48
, SUM(position_perc_rolling_48) / SUM(tournament_count_rolling_48) AS position_perc_rolling_48
, SUM(points_perc_rolling_48) / SUM(tournament_count_rolling_48) AS points_perc_rolling_48
, SUM(tournament_count_rolling_all_time) AS tournament_count_rolling_all_time
, SUM(points_rolling_all_time) AS points_rolling_all_time
, SUM(position_perc_rolling_all_time) / SUM(tournament_count_rolling_all_time) AS position_perc_rolling_all_time
, SUM(points_perc_rolling_all_time) / SUM(tournament_count_rolling_all_time) AS points_perc_rolling_all_time
FROM {{ ref('stg_player_locations') }} stg_player_locations
WHERE geography IS NOT NULL
AND tournament_count_rolling_all_time > 0
GROUP BY 1,2
ORDER BY 2,1


{{
  config({
        "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                        --  add INDEX index_geography (geography(255))
                        --, add INDEX index_player_id (player_id)'
    })
}}
