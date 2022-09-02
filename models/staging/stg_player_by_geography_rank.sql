SELECT
    stg_player_by_geography.geography
  , stg_player_by_geography.player_id
  , geography_12_rank.tournament_count_rolling_12
  , geography_12_rank.tournament_count_rolling_12_rank
  , geography_12_rank.points_rolling_12
  , geography_12_rank.position_perc_rolling_12
  , geography_12_rank.points_perc_rolling_12
  , geography_48_rank.tournament_count_rolling_48
  , geography_48_rank.tournament_count_rolling_48_rank
  , geography_48_rank.points_rolling_48
  , geography_48_rank.position_perc_rolling_48
  , geography_48_rank.points_perc_rolling_48
  , geography_all_time_rank.tournament_count_rolling_all_time
  , geography_all_time_rank.tournament_count_rolling_all_time_rank
  , geography_all_time_rank.points_rolling_all_time
  , geography_all_time_rank.position_perc_rolling_all_time
  , geography_all_time_rank.points_perc_rolling_all_time
  FROM {{ ref('stg_player_by_geography') }} stg_player_by_geography
  LEFT JOIN
      (
          SELECT
            stg_player_by_geography.geography
          , stg_player_by_geography.player_id
          , stg_player_by_geography.tournament_count_rolling_12
          , CASE
              WHEN tournament_count_rolling_12 IS NOT NULL THEN
                  ( @counter_01 := IF ( @curr_player_01 = stg_player_by_geography.player_id, @counter_01 + 1, 1 ) )
              ELSE NULL
            END AS tournament_count_rolling_12_rank
          , stg_player_by_geography.points_rolling_12
          , stg_player_by_geography.position_perc_rolling_12
          , stg_player_by_geography.points_perc_rolling_12
          , ( @curr_player_01 := stg_player_by_geography.player_id ) AS curr_player_id
          FROM {{ ref('stg_player_by_geography') }} stg_player_by_geography
          WHERE tournament_count_rolling_12 > 0
          ORDER BY 2, 3 DESC
      ) AS geography_12_rank
  ON geography_12_rank.geography = stg_player_by_geography.geography
  AND geography_12_rank.player_id = stg_player_by_geography.player_id
  LEFT JOIN
      (
          SELECT
            stg_player_by_geography.geography
          , stg_player_by_geography.player_id
          , stg_player_by_geography.tournament_count_rolling_48
          , CASE
              WHEN tournament_count_rolling_48 IS NOT NULL THEN
                  ( @counter_02 := IF ( @curr_player_02 = stg_player_by_geography.player_id, @counter_02 + 1, 1 ) )
              ELSE NULL
            END AS tournament_count_rolling_48_rank
          , stg_player_by_geography.points_rolling_48
          , stg_player_by_geography.position_perc_rolling_48
          , stg_player_by_geography.points_perc_rolling_48
          , ( @curr_player_02 := stg_player_by_geography.player_id ) AS curr_player_id
          FROM {{ ref('stg_player_by_geography') }} stg_player_by_geography
          WHERE tournament_count_rolling_48 > 0
          ORDER BY 2, 3 DESC
      ) AS geography_48_rank
  ON geography_48_rank.geography = stg_player_by_geography.geography
  AND geography_48_rank.player_id = stg_player_by_geography.player_id
  LEFT JOIN
      (
          SELECT
            stg_player_by_geography.geography
          , stg_player_by_geography.player_id
          , stg_player_by_geography.tournament_count_rolling_all_time
          , CASE
              WHEN tournament_count_rolling_all_time IS NOT NULL THEN
                  ( @counter_03 := IF ( @curr_player_03 = stg_player_by_geography.player_id, @counter_03 + 1, 1 ) )
              ELSE NULL
            END AS tournament_count_rolling_all_time_rank
          , stg_player_by_geography.points_rolling_all_time
          , stg_player_by_geography.position_perc_rolling_all_time
          , stg_player_by_geography.points_perc_rolling_all_time
          , ( @curr_player_03 := stg_player_by_geography.player_id ) AS curr_player_id
          FROM {{ ref('stg_player_by_geography') }} stg_player_by_geography
          WHERE tournament_count_rolling_all_time > 0
          ORDER BY 2, 3 DESC
      ) AS geography_all_time_rank
  ON geography_all_time_rank.geography = stg_player_by_geography.geography
  AND geography_all_time_rank.player_id = stg_player_by_geography.player_id

 {{
   config({
     "pre-hook": 'SET @counter_01 = 0, @curr_player_01 = 0, @counter_02 = 0, @curr_player_02 = 0, @counter_03 = 0, @curr_player_03 = 0',
     "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                       add INDEX index_geography (geography(255))
                     , add INDEX index_player_id (player_id)'
     })
 }}
