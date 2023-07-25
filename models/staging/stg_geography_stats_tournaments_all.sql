SELECT
  SUM(stg_geography_stats_tournaments.count_tournament_wpprtunist::decimal) / SUM(stg_geography_stats_tournaments.count_tournaments::decimal) AS all_perc_tournament_wpprtunist
, SUM(stg_geography_stats_tournaments.count_tournament_traveler::decimal) / SUM(stg_geography_stats_tournaments.count_tournaments::decimal) AS all_perc_tournament_traveler
, SUM(stg_geography_stats_tournaments.count_tournament_localsupporter::decimal) / SUM(stg_geography_stats_tournaments.count_tournaments::decimal) AS all_perc_tournament_localsupporter
, SUM(stg_geography_stats_tournaments.count_tournament_onetimer::decimal) / SUM(stg_geography_stats_tournaments.count_tournaments::decimal) AS all_perc_tournament_onetimer
, SUM(stg_geography_stats_tournaments.count_tournament_mixedpersona::decimal) / SUM(stg_geography_stats_tournaments.count_tournaments::decimal) AS all_perc_tournament_mixedpersona
, SUM(stg_geography_stats_tournaments.count_tournament_nonactive::decimal) / SUM(stg_geography_stats_tournaments.count_tournaments::decimal) AS all_perc_tournament_nonactive
, SUM(stg_geography_stats_tournaments.count_tournament_wpprtunist) AS all_tournament_wpprtunist
, SUM(stg_geography_stats_tournaments.count_tournament_traveler) AS all_tournament_traveler
, SUM(stg_geography_stats_tournaments.count_tournament_localsupporter) AS all_tournament_localsupporter
, SUM(stg_geography_stats_tournaments.count_tournament_onetimer) AS all_tournament_onetimer
, SUM(stg_geography_stats_tournaments.count_tournament_mixedpersona) AS all_tournament_mixedpersona
, SUM(stg_geography_stats_tournaments.count_tournament_nonactive) AS all_tournament_nonactive
, SUM(stg_geography_stats_tournaments.count_tournaments) AS all_tournaments



FROM {{ ref('stg_geography_stats_tournaments') }} stg_geography_stats_tournaments
