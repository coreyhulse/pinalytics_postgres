SELECT
  stg_geography_stats_points.geography

, stg_geography_stats_points.points_01_month
, stg_geography_stats_points.points_03_month
, stg_geography_stats_points.points_12_month
, stg_geography_stats_points.points_24_month
, stg_geography_stats_points.points_36_month
, stg_geography_stats_points.points_48_month
, stg_geography_stats_points.points_60_month
, stg_geography_stats_points.points_alltime

, stg_geography_stats_points.points_winner_01_month
, stg_geography_stats_points.points_winner_03_month
, stg_geography_stats_points.points_winner_12_month
, stg_geography_stats_points.points_winner_24_month
, stg_geography_stats_points.points_winner_36_month
, stg_geography_stats_points.points_winner_48_month
, stg_geography_stats_points.points_winner_60_month
, stg_geography_stats_points.points_winner_alltime

, stg_geography_stats_points.tournaments_01_month
, stg_geography_stats_points.tournaments_03_month
, stg_geography_stats_points.tournaments_12_month
, stg_geography_stats_points.tournaments_24_month
, stg_geography_stats_points.tournaments_36_month
, stg_geography_stats_points.tournaments_48_month
, stg_geography_stats_points.tournaments_60_month
, stg_geography_stats_points.tournaments_alltime

, stg_geography_stats_players.count_player_wpprtunist AS count_player_wpprtunist
, stg_geography_stats_players.count_player_traveler AS count_player_traveler
, stg_geography_stats_players.count_player_localsupporter AS count_player_localsupporter
, stg_geography_stats_players.count_player_onetimer AS count_player_onetimer
, stg_geography_stats_players.count_player_nonactive AS count_player_nonactive
, stg_geography_stats_players.count_players AS count_players

, (stg_geography_stats_players.count_player_wpprtunist) / (stg_geography_stats_players.count_players) AS perc_player_wpprtunist
, (stg_geography_stats_players.count_player_traveler) / (stg_geography_stats_players.count_players) AS perc_player_traveler
, (stg_geography_stats_players.count_player_localsupporter) / (stg_geography_stats_players.count_players) AS perc_player_localsupporter
, (stg_geography_stats_players.count_player_onetimer) / (stg_geography_stats_players.count_players) AS perc_player_onetimer
, (stg_geography_stats_players.count_player_nonactive) / (stg_geography_stats_players.count_players) AS perc_player_nonactive

, stg_geography_stats_players_all.all_perc_player_wpprtunist
, stg_geography_stats_players_all.all_perc_player_traveler
, stg_geography_stats_players_all.all_perc_player_localsupporter
, stg_geography_stats_players_all.all_perc_player_onetimer
, stg_geography_stats_players_all.all_perc_player_nonactive

, stg_geography_stats_tournaments.count_tournament_wpprtunist AS count_tournament_wpprtunist
, stg_geography_stats_tournaments.count_tournament_traveler AS count_tournament_traveler
, stg_geography_stats_tournaments.count_tournament_localsupporter AS count_tournament_localsupporter
, stg_geography_stats_tournaments.count_tournament_onetimer AS count_tournament_onetimer
, stg_geography_stats_tournaments.count_tournament_nonactive AS count_tournament_nonactive
, stg_geography_stats_tournaments.count_tournaments AS count_tournaments

, (stg_geography_stats_tournaments.count_tournament_wpprtunist) / (stg_geography_stats_tournaments.count_tournaments) AS perc_tournament_wpprtunist
, (stg_geography_stats_tournaments.count_tournament_traveler) / (stg_geography_stats_tournaments.count_tournaments) AS perc_tournament_traveler
, (stg_geography_stats_tournaments.count_tournament_localsupporter) / (stg_geography_stats_tournaments.count_tournaments) AS perc_tournament_localsupporter
, (stg_geography_stats_tournaments.count_tournament_onetimer) / (stg_geography_stats_tournaments.count_tournaments) AS perc_tournament_onetimer
, (stg_geography_stats_tournaments.count_tournament_nonactive) / (stg_geography_stats_tournaments.count_tournaments) AS perc_tournament_nonactive

, stg_geography_stats_tournaments_all.all_perc_tournament_wpprtunist
, stg_geography_stats_tournaments_all.all_perc_tournament_traveler
, stg_geography_stats_tournaments_all.all_perc_tournament_localsupporter
, stg_geography_stats_tournaments_all.all_perc_tournament_onetimer
, stg_geography_stats_tournaments_all.all_perc_tournament_nonactive

FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
LEFT JOIN {{ ref('stg_geography_stats_players') }} stg_geography_stats_players
ON stg_geography_stats_points.geography = stg_geography_stats_players.geography
LEFT JOIN {{ ref('stg_geography_stats_tournaments') }} stg_geography_stats_tournaments
ON stg_geography_stats_points.geography = stg_geography_stats_tournaments.geography
LEFT JOIN {{ ref('stg_geography_stats_players_all') }} stg_geography_stats_players_all
ON 1=1
LEFT JOIN {{ ref('stg_geography_stats_tournaments_all') }} stg_geography_stats_tournaments_all
ON 1=1

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add PRIMARY KEY(geography(255))'
    })
}}
