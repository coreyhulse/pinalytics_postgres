SELECT
  SUM(stg_geography_stats_players.count_player_wpprtunist) / SUM(stg_geography_stats_players.count_players) AS all_perc_player_wpprtunist
, SUM(stg_geography_stats_players.count_player_traveler) / SUM(stg_geography_stats_players.count_players) AS all_perc_player_traveler
, SUM(stg_geography_stats_players.count_player_localsupporter) / SUM(stg_geography_stats_players.count_players) AS all_perc_player_localsupporter
, SUM(stg_geography_stats_players.count_player_onetimer) / SUM(stg_geography_stats_players.count_players) AS all_perc_player_onetimer
, SUM(stg_geography_stats_players.count_player_nonactive) / SUM(stg_geography_stats_players.count_players) AS all_perc_player_nonactive
, SUM(stg_geography_stats_players.count_player_wpprtunist) AS all_player_wpprtunist
, SUM(stg_geography_stats_players.count_player_traveler) AS all_player_traveler
, SUM(stg_geography_stats_players.count_player_localsupporter) AS all_player_localsupporter
, SUM(stg_geography_stats_players.count_player_onetimer) AS all_player_onetimer
, SUM(stg_geography_stats_players.count_player_nonactive) AS all_player_nonactive

FROM {{ ref('stg_geography_stats_players') }} stg_geography_stats_players
