SELECT
  player_id
, MAX(fct_ifpa_tournament_results.date) AS most_recent_tournament_date
, CASE WHEN SUM(fct_ifpa_tournament_results.rolling_01_month) >= 1 THEN 1 ELSE 0 END AS rolling_01_month
, CASE WHEN SUM(fct_ifpa_tournament_results.rolling_03_month) >= 1 THEN 1 ELSE 0 END AS rolling_03_month
, CASE WHEN SUM(fct_ifpa_tournament_results.rolling_12_month) >= 1 THEN 1 ELSE 0 END AS rolling_12_month
, CASE WHEN SUM(fct_ifpa_tournament_results.rolling_24_month) >= 1 THEN 1 ELSE 0 END AS rolling_24_month
, CASE WHEN SUM(fct_ifpa_tournament_results.rolling_36_month) >= 1 THEN 1 ELSE 0 END AS rolling_36_month
, CASE WHEN SUM(fct_ifpa_tournament_results.rolling_48_month) >= 1 THEN 1 ELSE 0 END AS rolling_48_month
, CASE WHEN SUM(fct_ifpa_tournament_results.rolling_60_month) >= 1 THEN 1 ELSE 0 END AS rolling_60_month
, CASE WHEN SUM(fct_ifpa_tournament_results.rolling_all_time) >= 1 THEN 1 ELSE 0 END AS rolling_all_time
FROM {{ ref('fct_ifpa_tournament_results') }} fct_ifpa_tournament_results
GROUP BY 1