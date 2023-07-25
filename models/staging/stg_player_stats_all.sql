WITH player_summary AS (

SELECT
  COUNT(DISTINCT CASE WHEN stg_player_stats.player_persona = 'WPPRtunist' THEN stg_player_stats.player_id ELSE NULL END) AS count_player_wpprtunist
, COUNT(DISTINCT CASE WHEN stg_player_stats.player_persona = 'Traveler' THEN stg_player_stats.player_id ELSE NULL END) AS count_player_traveler
, COUNT(DISTINCT CASE WHEN stg_player_stats.player_persona = 'Local Supporter' THEN stg_player_stats.player_id ELSE NULL END) AS count_player_localsupporter
, COUNT(DISTINCT CASE WHEN stg_player_stats.player_persona = 'One-Timer' THEN stg_player_stats.player_id ELSE NULL END) AS count_player_onetimer
, COUNT(DISTINCT CASE WHEN stg_player_stats.player_persona = 'Non-Active' THEN stg_player_stats.player_id ELSE NULL END) AS count_player_nonactive
, COUNT(DISTINCT stg_player_stats.player_id) AS count_players
FROM {{ ref('stg_player_stats') }} stg_player_stats
WHERE rolling_48_month = 1

)

SELECT
  SUM(player_summary.count_player_wpprtunist::decimal) / SUM(player_summary.count_players::decimal) AS all_perc_player_wpprtunist
, SUM(player_summary.count_player_traveler::decimal) / SUM(player_summary.count_players::decimal) AS all_perc_player_traveler
, SUM(player_summary.count_player_localsupporter::decimal) / SUM(player_summary.count_players::decimal) AS all_perc_player_localsupporter
, SUM(player_summary.count_player_onetimer::decimal) / SUM(player_summary.count_players::decimal) AS all_perc_player_onetimer
, SUM(player_summary.count_player_nonactive::decimal) / SUM(player_summary.count_players::decimal) AS all_perc_player_nonactive
, SUM(player_summary.count_player_wpprtunist) AS all_player_wpprtunist
, SUM(player_summary.count_player_traveler) AS all_player_traveler
, SUM(player_summary.count_player_localsupporter) AS all_player_localsupporter
, SUM(player_summary.count_player_onetimer) AS all_player_onetimer
, SUM(player_summary.count_player_nonactive) AS all_player_nonactive
FROM player_summary
