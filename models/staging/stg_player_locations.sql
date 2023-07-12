SELECT
  fct_ifpa_tournament_results.ranking_system
, fct_ifpa_tournament_results.player_id
, fct_ifpa_tournament_results.city
, fct_ifpa_tournament_results.stateprov
, fct_ifpa_tournament_results.city_state
, fct_ifpa_tournament_results.postal_code
, fct_ifpa_tournament_results.geography
, fct_ifpa_tournament_results.country_name
, COUNT(DISTINCT CASE WHEN rolling_12_month = 1 THEN tournament_id ELSE NULL END) AS tournament_count_rolling_12
, SUM(CASE WHEN rolling_12_month = 1 THEN points ELSE NULL END) AS points_rolling_12
, SUM(CASE WHEN rolling_12_month = 1 THEN position_percentage ELSE NULL END) / COUNT(DISTINCT CASE WHEN rolling_12_month = 1 THEN tournament_id ELSE NULL END) AS position_perc_rolling_12
, SUM(CASE WHEN rolling_12_month = 1 THEN points_percentage ELSE NULL END) / COUNT(DISTINCT CASE WHEN rolling_12_month = 1 THEN tournament_id ELSE NULL END) AS points_perc_rolling_12
, COUNT(DISTINCT CASE WHEN rolling_48_month = 1 THEN tournament_id ELSE NULL END) AS tournament_count_rolling_48
, SUM(CASE WHEN rolling_48_month = 1 THEN points ELSE NULL END) AS points_rolling_48
, SUM(CASE WHEN rolling_48_month = 1 THEN position_percentage ELSE NULL END) / COUNT(DISTINCT CASE WHEN rolling_48_month = 1 THEN tournament_id ELSE NULL END) AS position_perc_rolling_48
, SUM(CASE WHEN rolling_48_month = 1 THEN points_percentage ELSE NULL END) / COUNT(DISTINCT CASE WHEN rolling_48_month = 1 THEN tournament_id ELSE NULL END) AS points_perc_rolling_48
, COUNT(tournament_id) AS tournament_count_rolling_all_time
, SUM(points) AS points_rolling_all_time
, SUM(position_percentage) / COUNT(DISTINCT tournament_id) AS position_perc_rolling_all_time
, SUM(points_percentage) / COUNT(DISTINCT tournament_id) AS points_perc_rolling_all_time
FROM {{ ref('fct_ifpa_tournament_results') }} fct_ifpa_tournament_results
WHERE player_id <> 0
GROUP BY 1,2,3,4,5,6,7,8

/*
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                    --  add INDEX index_player (player_id)
                    --, add INDEX index_geography (geography(255))
                    '
    })
*/
