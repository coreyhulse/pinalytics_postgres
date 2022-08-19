SELECT
  stg_geography_by_date.geography
, stg_calendar.yearmonth
, stg_calendar.rolling_01_month
, stg_calendar.rolling_03_month
, stg_calendar.rolling_12_month
, stg_calendar.rolling_24_month
, stg_calendar.rolling_36_month
, stg_calendar.rolling_48_month
, stg_calendar.rolling_60_month
, SUM(points) AS points
, SUM(points_winner) AS points_winner
, SUM(tournaments) AS tournaments

FROM {{ ref('stg_geography_by_date') }} stg_geography_by_date
LEFT JOIN {{ ref('stg_calendar') }} stg_calendar
ON stg_geography_by_date.date = stg_calendar.date
GROUP BY 1,2,3,4,5,6,7,8,9

{{
  config({
        "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                          add INDEX index_geography (geography(255))
                        , add INDEX index_yearmonth (yearmonth(7))'
    })
}}
