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
          , RANK() OVER (ORDER BY points_01_month DESC) AS points_01_month_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY points_01_month DESC
      ) AS points_01_rank
  ON points_01_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , RANK() OVER (ORDER BY points_12_month DESC) AS points_12_month_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY points_12_month DESC
      ) AS points_12_rank
  ON points_12_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , RANK() OVER (ORDER BY points_48_month DESC) AS points_48_month_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY points_48_month DESC
      ) AS points_48_rank
  ON points_48_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , RANK() OVER (ORDER BY points_alltime DESC) AS points_alltime_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY points_alltime DESC
      ) AS points_alltime_rank
  ON points_alltime_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , RANK() OVER (ORDER BY points_winner_01_month DESC) AS points_winner_01_month_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY points_winner_01_month DESC
      ) AS points_winner_01_rank
  ON points_winner_01_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , RANK() OVER (ORDER BY points_winner_12_month DESC) AS points_winner_12_month_rank 
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY points_winner_12_month DESC
      ) AS points_winner_12_rank
  ON points_winner_12_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , RANK() OVER (ORDER BY points_winner_48_month DESC) AS points_winner_48_month_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY points_winner_48_month DESC
      ) AS points_winner_48_rank
  ON points_winner_48_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , RANK() OVER (ORDER BY points_winner_alltime DESC) AS points_winner_alltime_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY points_winner_alltime DESC
      ) AS points_winner_alltime_rank
  ON points_winner_alltime_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , RANK() OVER (ORDER BY tournaments_01_month DESC) AS tournaments_01_month_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY tournaments_01_month DESC
      ) AS tournaments_01_rank
  ON tournaments_01_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , RANK() OVER (ORDER BY tournaments_12_month DESC) AS tournaments_12_month_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY tournaments_12_month DESC
      ) AS tournaments_12_rank
  ON tournaments_12_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , RANK() OVER (ORDER BY tournaments_48_month DESC) AS tournaments_48_month_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY tournaments_48_month DESC
      ) AS tournaments_48_rank
  ON tournaments_48_rank.geography = stg_geography_stats_points.geography
  LEFT JOIN
      (
          SELECT
            geography
          , RANK() OVER (ORDER BY tournaments_alltime DESC) AS tournaments_alltime_rank
          FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
          ORDER BY tournaments_alltime DESC
      ) AS tournaments_alltime_rank
  ON tournaments_alltime_rank.geography = stg_geography_stats_points.geography

 {{
   config({
     "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add PRIMARY KEY(geography)'
     })
 }}
