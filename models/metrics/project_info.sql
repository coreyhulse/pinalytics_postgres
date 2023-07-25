WITH max_date AS (
SELECT
    MAX(date) AS data_max_date
FROM {{ ref('fct_ifpa_tournaments') }}
WHERE rolling_all_time = 1
)
,
run_date AS (
SELECT
    MAX(date_trunc('day', line_timestamp))::date AS run_date
FROM {{ ref('fct_ifpa_tournament_results') }}
WHERE rolling_all_time = 1
)

SELECT
      '{{ var('version_number') }}' AS version
    , data_max_date AS data_max_date
    , run_date AS run_date
FROM max_date
LEFT JOIN run_date
ON 1=1
