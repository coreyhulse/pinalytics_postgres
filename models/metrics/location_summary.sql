SELECT
  stg_location.location
, SUM(CASE WHEN rolling_01_month = 1 THEN points ELSE NULL END) AS points_01_month
, SUM(CASE WHEN rolling_03_month = 1 THEN points ELSE NULL END) AS points_03_month
, SUM(CASE WHEN rolling_12_month = 1 THEN points ELSE NULL END) AS points_12_month
, SUM(CASE WHEN rolling_24_month = 1 THEN points ELSE NULL END) AS points_24_month
, SUM(CASE WHEN rolling_36_month = 1 THEN points ELSE NULL END) AS points_36_month
, SUM(CASE WHEN rolling_48_month = 1 THEN points ELSE NULL END) AS points_48_month
, SUM(CASE WHEN rolling_60_month = 1 THEN points ELSE NULL END) AS points_60_month
, SUM(points) AS points_alltime

, SUM(CASE WHEN rolling_01_month = 1 AND position = 1 THEN points ELSE NULL END) AS points_winner_01_month
, SUM(CASE WHEN rolling_03_month = 1 AND position = 1 THEN points ELSE NULL END) AS points_winner_03_month
, SUM(CASE WHEN rolling_12_month = 1 AND position = 1 THEN points ELSE NULL END) AS points_winner_12_month
, SUM(CASE WHEN rolling_24_month = 1 AND position = 1 THEN points ELSE NULL END) AS points_winner_24_month
, SUM(CASE WHEN rolling_36_month = 1 AND position = 1 THEN points ELSE NULL END) AS points_winner_36_month
, SUM(CASE WHEN rolling_48_month = 1 AND position = 1 THEN points ELSE NULL END) AS points_winner_48_month
, SUM(CASE WHEN rolling_60_month = 1 AND position = 1 THEN points ELSE NULL END) AS points_winner_60_month
, SUM(CASE WHEN position = 1 THEN points ELSE NULL END) AS points_winner_all_time

, COUNT(DISTINCT CASE WHEN rolling_01_month = 1 THEN tournament_id ELSE NULL END) AS tournaments_01_month
, COUNT(DISTINCT CASE WHEN rolling_03_month = 1 THEN tournament_id ELSE NULL END) AS tournaments_03_month
, COUNT(DISTINCT CASE WHEN rolling_12_month = 1 THEN tournament_id ELSE NULL END) AS tournaments_12_month
, COUNT(DISTINCT CASE WHEN rolling_24_month = 1 THEN tournament_id ELSE NULL END) AS tournaments_24_month
, COUNT(DISTINCT CASE WHEN rolling_36_month = 1 THEN tournament_id ELSE NULL END) AS tournaments_36_month
, COUNT(DISTINCT CASE WHEN rolling_48_month = 1 THEN tournament_id ELSE NULL END) AS tournaments_48_month
, COUNT(DISTINCT CASE WHEN rolling_60_month = 1 THEN tournament_id ELSE NULL END) AS tournaments_60_month
, COUNT(DISTINCT tournament_id) AS tournaments_alltime



FROM {{ ref('stg_locations') }} stg_location
LEFT JOIN {{ ref('ifpa_tournament_results') }} ifpa_tournament_results
ON stg_location.location = ifpa_tournament_results.location
LEFT JOIN {{ ref('stg_calendar') }} stg_calendar
ON ifpa_tournament_results.event_start_date = stg_calendar.date
GROUP BY 1

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add PRIMARY KEY(location(255))'
    })
}}
