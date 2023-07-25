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

, stg_player_count_top_geography.player_count_top_geography_rolling_48

, (stg_geography_stats_points.points_48_month::decimal / CASE WHEN stg_geography_stats_points.points_winner_48_month = 0 THEN NULL ELSE points_winner_48_month::decimal END) AS points_winner_ratio
, (stg_geography_stats_players.count_players::decimal / CASE WHEN stg_player_count_top_geography.player_count_top_geography_rolling_48 = 0 THEN NULL ELSE player_count_top_geography_rolling_48::decimal END) AS traveler_home_ratio

, (stg_geography_stats_players.count_player_wpprtunist::decimal) / (stg_geography_stats_players.count_players::decimal) AS perc_player_wpprtunist
, (stg_geography_stats_players.count_player_traveler::decimal) / (stg_geography_stats_players.count_players::decimal) AS perc_player_traveler
, (stg_geography_stats_players.count_player_localsupporter::decimal) / (stg_geography_stats_players.count_players::decimal) AS perc_player_localsupporter
, (stg_geography_stats_players.count_player_onetimer::decimal) / (stg_geography_stats_players.count_players::decimal) AS perc_player_onetimer
, (stg_geography_stats_players.count_player_nonactive::decimal) / (stg_geography_stats_players.count_players::decimal) AS perc_player_nonactive

, stg_player_stats_all.all_perc_player_wpprtunist
, stg_player_stats_all.all_perc_player_traveler
, stg_player_stats_all.all_perc_player_localsupporter
, stg_player_stats_all.all_perc_player_onetimer
, stg_player_stats_all.all_perc_player_nonactive

, stg_geography_stats_tournaments.count_tournament_wpprtunist AS count_tournament_wpprtunist
, stg_geography_stats_tournaments.count_tournament_traveler AS count_tournament_traveler
, stg_geography_stats_tournaments.count_tournament_localsupporter AS count_tournament_localsupporter
, stg_geography_stats_tournaments.count_tournament_onetimer AS count_tournament_onetimer
, stg_geography_stats_tournaments.count_tournament_nonactive AS count_tournament_nonactive
, stg_geography_stats_tournaments.count_tournament_mixedpersona AS count_tournament_mixedpersona
, stg_geography_stats_tournaments.count_tournaments AS count_tournaments

, (stg_geography_stats_tournaments.count_tournament_wpprtunist::decimal) / (stg_geography_stats_tournaments.count_tournaments::decimal) AS perc_tournament_wpprtunist
, (stg_geography_stats_tournaments.count_tournament_traveler::decimal) / (stg_geography_stats_tournaments.count_tournaments::decimal) AS perc_tournament_traveler
, (stg_geography_stats_tournaments.count_tournament_localsupporter::decimal) / (stg_geography_stats_tournaments.count_tournaments::decimal) AS perc_tournament_localsupporter
, (stg_geography_stats_tournaments.count_tournament_onetimer::decimal) / (stg_geography_stats_tournaments.count_tournaments::decimal) AS perc_tournament_onetimer
, (stg_geography_stats_tournaments.count_tournament_mixedpersona::decimal) / (stg_geography_stats_tournaments.count_tournaments::decimal) AS perc_tournament_mixedpersona
, (stg_geography_stats_tournaments.count_tournament_nonactive::decimal) / (stg_geography_stats_tournaments.count_tournaments::decimal) AS perc_tournament_nonactive

, stg_geography_stats_tournaments_all.all_perc_tournament_wpprtunist
, stg_geography_stats_tournaments_all.all_perc_tournament_traveler
, stg_geography_stats_tournaments_all.all_perc_tournament_localsupporter
, stg_geography_stats_tournaments_all.all_perc_tournament_onetimer
, stg_geography_stats_tournaments_all.all_perc_tournament_mixedpersona
, stg_geography_stats_tournaments_all.all_perc_tournament_nonactive

, geography_rank.points_01_month_rank
, geography_rank.points_12_month_rank
, geography_rank.points_48_month_rank
, geography_rank.points_alltime_rank
, geography_rank.points_winner_01_month_rank
, geography_rank.points_winner_12_month_rank
, geography_rank.points_winner_48_month_rank
, geography_rank.points_winner_alltime_rank
, geography_rank.tournaments_01_month_rank
, geography_rank.tournaments_12_month_rank
, geography_rank.tournaments_48_month_rank
, geography_rank.tournaments_alltime_rank

, stg_geography_player_bins.player_bin_010
, stg_geography_player_bins.player_bin_020
, stg_geography_player_bins.player_bin_030
, stg_geography_player_bins.player_bin_040
, stg_geography_player_bins.player_bin_050
, stg_geography_player_bins.player_bin_060
, stg_geography_player_bins.player_bin_070
, stg_geography_player_bins.player_bin_080
, stg_geography_player_bins.player_bin_090
, stg_geography_player_bins.player_bin_100
, stg_geography_player_bins.player_bin_100_plus

, stg_geography_player_bins.player_bin_010_perc
, stg_geography_player_bins.player_bin_020_perc
, stg_geography_player_bins.player_bin_030_perc
, stg_geography_player_bins.player_bin_040_perc
, stg_geography_player_bins.player_bin_050_perc
, stg_geography_player_bins.player_bin_060_perc
, stg_geography_player_bins.player_bin_070_perc
, stg_geography_player_bins.player_bin_080_perc
, stg_geography_player_bins.player_bin_090_perc
, stg_geography_player_bins.player_bin_100_perc
, stg_geography_player_bins.player_bin_100_plus_perc

, stg_geography_player_bins_all.all_player_bin_010_perc
, stg_geography_player_bins_all.all_player_bin_020_perc
, stg_geography_player_bins_all.all_player_bin_030_perc
, stg_geography_player_bins_all.all_player_bin_040_perc
, stg_geography_player_bins_all.all_player_bin_050_perc
, stg_geography_player_bins_all.all_player_bin_060_perc
, stg_geography_player_bins_all.all_player_bin_070_perc
, stg_geography_player_bins_all.all_player_bin_080_perc
, stg_geography_player_bins_all.all_player_bin_090_perc
, stg_geography_player_bins_all.all_player_bin_100_perc
, stg_geography_player_bins_all.all_player_bin_100_plus_perc






FROM {{ ref('stg_geography_stats_points') }} stg_geography_stats_points
LEFT JOIN {{ ref('stg_geography_stats_players') }} stg_geography_stats_players
ON stg_geography_stats_points.geography = stg_geography_stats_players.geography
LEFT JOIN {{ ref('stg_player_count_top_geography') }} stg_player_count_top_geography
ON stg_geography_stats_points.geography = stg_player_count_top_geography.geography
LEFT JOIN {{ ref('stg_geography_stats_tournaments') }} stg_geography_stats_tournaments
ON stg_geography_stats_points.geography = stg_geography_stats_tournaments.geography
LEFT JOIN {{ ref('geography_rank') }} geography_rank
ON stg_geography_stats_points.geography = geography_rank.geography
LEFT JOIN {{ ref('stg_geography_player_bins') }} stg_geography_player_bins
ON stg_geography_stats_points.geography = stg_geography_player_bins.geography
LEFT JOIN {{ ref('stg_player_stats_all') }} stg_player_stats_all
ON 1=1
LEFT JOIN {{ ref('stg_geography_stats_tournaments_all') }} stg_geography_stats_tournaments_all
ON 1=1
LEFT JOIN {{ ref('stg_geography_player_bins_all') }} stg_geography_player_bins_all
ON 1=1


{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add PRIMARY KEY(geography)'
    })
}}
