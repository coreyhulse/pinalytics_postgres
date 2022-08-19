SELECT
  geography_by_month.geography
, SUM(CASE WHEN rolling_01_month = 1 THEN points ELSE NULL END) AS points_01_month
, SUM(CASE WHEN rolling_03_month = 1 THEN points ELSE NULL END) AS points_03_month
, SUM(CASE WHEN rolling_12_month = 1 THEN points ELSE NULL END) AS points_12_month
, SUM(CASE WHEN rolling_24_month = 1 THEN points ELSE NULL END) AS points_24_month
, SUM(CASE WHEN rolling_36_month = 1 THEN points ELSE NULL END) AS points_36_month
, SUM(CASE WHEN rolling_48_month = 1 THEN points ELSE NULL END) AS points_48_month
, SUM(CASE WHEN rolling_60_month = 1 THEN points ELSE NULL END) AS points_60_month
, SUM(points) AS points_alltime

, SUM(CASE WHEN rolling_01_month = 1 THEN points_winner ELSE NULL END) AS points_winner_01_month
, SUM(CASE WHEN rolling_03_month = 1 THEN points_winner ELSE NULL END) AS points_winner_03_month
, SUM(CASE WHEN rolling_12_month = 1 THEN points_winner ELSE NULL END) AS points_winner_12_month
, SUM(CASE WHEN rolling_24_month = 1 THEN points_winner ELSE NULL END) AS points_winner_24_month
, SUM(CASE WHEN rolling_36_month = 1 THEN points_winner ELSE NULL END) AS points_winner_36_month
, SUM(CASE WHEN rolling_48_month = 1 THEN points_winner ELSE NULL END) AS points_winner_48_month
, SUM(CASE WHEN rolling_60_month = 1 THEN points_winner ELSE NULL END) AS points_winner_60_month
, SUM(points_winner) AS points_winner_alltime

, SUM(CASE WHEN rolling_01_month = 1 THEN tournaments ELSE NULL END) AS tournaments_01_month
, SUM(CASE WHEN rolling_03_month = 1 THEN tournaments ELSE NULL END) AS tournaments_03_month
, SUM(CASE WHEN rolling_12_month = 1 THEN tournaments ELSE NULL END) AS tournaments_12_month
, SUM(CASE WHEN rolling_24_month = 1 THEN tournaments ELSE NULL END) AS tournaments_24_month
, SUM(CASE WHEN rolling_36_month = 1 THEN tournaments ELSE NULL END) AS tournaments_36_month
, SUM(CASE WHEN rolling_48_month = 1 THEN tournaments ELSE NULL END) AS tournaments_48_month
, SUM(CASE WHEN rolling_60_month = 1 THEN tournaments ELSE NULL END) AS tournaments_60_month
, SUM(tournaments) AS tournaments_alltime



FROM {{ ref('geography_by_month') }} geography_by_month
GROUP BY 1

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add PRIMARY KEY(geography(255))'
    })
}}
