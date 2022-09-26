SELECT
  fct_ifpa_tournament_results.tournament_id
, COUNT(DISTINCT CASE WHEN stg_player_stats.player_persona = 'WPPRtunist' THEN fct_ifpa_tournament_results.player_id ELSE NULL END) AS count_wpprtunist
, COUNT(DISTINCT CASE WHEN stg_player_stats.player_persona = 'Traveler' THEN fct_ifpa_tournament_results.player_id ELSE NULL END) AS count_traveler
, COUNT(DISTINCT CASE WHEN stg_player_stats.player_persona = 'Local Supporter' THEN fct_ifpa_tournament_results.player_id ELSE NULL END) AS count_localsupporter
, COUNT(DISTINCT CASE WHEN stg_player_stats.player_persona = 'One-Timer' THEN fct_ifpa_tournament_results.player_id ELSE NULL END) AS count_onetimer
, COUNT(DISTINCT CASE WHEN stg_player_stats.player_persona = 'Non-Active' THEN fct_ifpa_tournament_results.player_id ELSE NULL END) AS count_nonactive
FROM {{ ref('fct_ifpa_tournament_results') }} fct_ifpa_tournament_results
LEFT JOIN {{ ref('stg_player_stats') }} stg_player_stats
ON fct_ifpa_tournament_results.player_id = stg_player_stats.player_id
GROUP BY 1

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add PRIMARY KEY(tournament_id)'
    })
}}
