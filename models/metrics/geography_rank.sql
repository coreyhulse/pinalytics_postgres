SELECT
    stg_geography_stats_points.geography
  , points_01_month_rank
  , points_12_month_rank
  , points_48_month_rank
  , points_alltime_rank
  , points_winner_01_month_rank
  , points_winner_12_month_rank
  , points_winner_48_month_rank
  , points_winner_alltime_rank
  , tournaments_01_month_rank
  , tournaments_12_month_rank
  , tournaments_48_month_rank
  , tournaments_alltime_rank
  FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
  LEFT JOIN
      (
          SELECT
            geography
          , CASE
              WHEN points_01_month IS NOT NULL THEN ( @counter_01 := @counter_01 + 1 )
              ELSE NULL
            END AS points_01_month_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY points_01_month DESC
      ) AS points_01_rank
  ON points_01_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , CASE
              WHEN points_12_month IS NOT NULL THEN ( @counter_02 := @counter_02 + 1 )
              ELSE NULL
            END AS points_12_month_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY points_12_month DESC
      ) AS points_12_rank
  ON points_12_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , CASE
              WHEN points_48_month IS NOT NULL THEN ( @counter_10 := @counter_10 + 1 )
              ELSE NULL
            END AS points_48_month_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY points_48_month DESC
      ) AS points_48_rank
  ON points_48_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , CASE
              WHEN points_alltime IS NOT NULL THEN ( @counter_03 := @counter_03 + 1 )
              ELSE NULL
            END AS points_alltime_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY points_alltime DESC
      ) AS points_alltime_rank
  ON points_alltime_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , CASE
              WHEN points_winner_01_month IS NOT NULL THEN ( @counter_04 := @counter_04 + 1 )
              ELSE NULL
            END AS points_winner_01_month_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY points_winner_01_month DESC
      ) AS points_winner_01_rank
  ON points_winner_01_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , CASE
              WHEN points_winner_12_month IS NOT NULL THEN ( @counter_05 := @counter_05 + 1 )
              ELSE NULL
            END AS points_winner_12_month_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY points_winner_12_month DESC
      ) AS points_winner_12_rank
  ON points_winner_12_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , CASE
              WHEN points_winner_48_month IS NOT NULL THEN ( @counter_11 := @counter_11 + 1 )
              ELSE NULL
            END AS points_winner_48_month_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY points_winner_48_month DESC
      ) AS points_winner_48_rank
  ON points_winner_48_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , CASE
              WHEN points_winner_alltime IS NOT NULL THEN ( @counter_06 := @counter_06 + 1 )
              ELSE NULL
            END AS points_winner_alltime_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY points_winner_alltime DESC
      ) AS points_winner_alltime_rank
  ON points_winner_alltime_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , CASE
              WHEN tournaments_01_month IS NOT NULL THEN ( @counter_07 := @counter_07 + 1 )
              ELSE NULL
            END AS tournaments_01_month_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY tournaments_01_month DESC
      ) AS tournaments_01_rank
  ON tournaments_01_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , CASE
              WHEN tournaments_12_month IS NOT NULL THEN ( @counter_08 := @counter_08 + 1 )
              ELSE NULL
            END AS tournaments_12_month_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY tournaments_12_month DESC
      ) AS tournaments_12_rank
  ON tournaments_12_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , CASE
              WHEN tournaments_48_month IS NOT NULL THEN ( @counter_12 := @counter_12 + 1 )
              ELSE NULL
            END AS tournaments_48_month_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY tournaments_48_month DESC
      ) AS tournaments_48_rank
  ON tournaments_48_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , CASE
              WHEN tournaments_alltime IS NOT NULL THEN ( @counter_09 := @counter_09 + 1 )
              ELSE NULL
            END AS tournaments_alltime_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY tournaments_alltime DESC
      ) AS tournaments_alltime_rank
  ON tournaments_alltime_rank.geography = stg_geography_stats_points.geography

 {{
   config({
     "pre-hook": 'SET @counter_01 = 0, @counter_02 = 0, @counter_03 = 0, @counter_04 = 0, @counter_05 = 0, @counter_06 = 0, @counter_07 = 0, @counter_08 = 0, @counter_09 = 0, @counter_10 = 0, @counter_11 = 0, @counter_12 = 0',
     "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add PRIMARY KEY(geography(255))'
     })
 }}
