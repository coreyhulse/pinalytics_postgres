SELECT
      COUNT(DISTINCT tournament_id) AS all_tournament_count
    , SUM(tournament_value) AS all_wppr_points
FROM {{ ref('fct_ifpa_tournaments') }} fct_ifpa_tournaments
WHERE rolling_48_month = 1
