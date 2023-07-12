SELECT
	date
  , EXTRACT(MONTH FROM date) AS month
  , EXTRACT(YEAR FROM date) AS year
  , CONCAT(EXTRACT(YEAR FROM date), '-', RIGHT(CONCAT('00', EXTRACT(MONTH FROM date)),2)) AS yearmonth
  , CASE WHEN EXTRACT(YEAR FROM CURRENT_DATE) = EXTRACT(YEAR FROM date) THEN 1 ELSE 0 END AS current_year_flag
  , DATE_TRUNC('month', CURRENT_DATE) AS date_check
  , CASE WHEN date < DATE_TRUNC('month', CURRENT_DATE) THEN 1 ELSE 0 END AS rolling_all_time
  , CASE WHEN date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '5 years') AND date < DATE_TRUNC('month', CURRENT_DATE) THEN 1 ELSE 0 END AS rolling_60_month
  , CASE WHEN date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '4 years') AND date < DATE_TRUNC('month', CURRENT_DATE) THEN 1 ELSE 0 END AS rolling_48_month
  , CASE WHEN date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 years') AND date < DATE_TRUNC('month', CURRENT_DATE) THEN 1 ELSE 0 END AS rolling_36_month
  , CASE WHEN date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '2 years') AND date < DATE_TRUNC('month', CURRENT_DATE) THEN 1 ELSE 0 END AS rolling_24_month
  , CASE WHEN date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 years') AND date < DATE_TRUNC('month', CURRENT_DATE) THEN 1 ELSE 0 END AS rolling_12_month
  , CASE WHEN date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months') AND date < DATE_TRUNC('month', CURRENT_DATE) THEN 1 ELSE 0 END AS rolling_03_month
  , CASE WHEN date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 months') AND date < DATE_TRUNC('month', CURRENT_DATE) THEN 1 ELSE 0 END AS rolling_01_month
  , CASE WHEN date >= DATE_TRUNC('month', CURRENT_DATE) THEN 1 ELSE 0 END AS future_dates
FROM {{ ref('date_spine') }}

{{
config({
  "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                    add PRIMARY KEY(date)
                --  , add INDEX index_date (date)
                --  , add INDEX index_yearmonth (yearmonth(7))'
  })
}}
