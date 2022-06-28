SELECT
  stg_location.location
, stg_calendar.yearmonth
, stg_calendar.rolling_01_month
, stg_calendar.rolling_03_month
, stg_calendar.rolling_12_month
, stg_calendar.rolling_24_month
, stg_calendar.rolling_36_month
, stg_calendar.rolling_48_month
, stg_calendar.rolling_60_month
, SUM(points) AS points
, SUM(CASE WHEN position = 1 THEN points ELSE NULL END) AS points_winner
, COUNT(DISTINCT tournament_id) AS tournaments

FROM {{ ref('stg_locations') }} stg_location
LEFT JOIN {{ ref('ifpa_tournament_results') }} ifpa_tournament_results
ON stg_location.location = ifpa_tournament_results.location
LEFT JOIN {{ ref('stg_calendar') }} stg_calendar
ON ifpa_tournament_results.event_start_date = stg_calendar.date
GROUP BY 1,2,3,4,5,6,7,8,9

{{
  config({
        "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add INDEX index_location (location(255))',
        "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add INDEX index_location (yearmonth(255))'
    })
}}
