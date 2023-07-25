SELECT
  SUM(tournament_count) AS all_tournament_count
, SUM(player_bin_010) AS all_player_bin_010
, SUM(player_bin_020) AS all_player_bin_020
, SUM(player_bin_030) AS all_player_bin_030
, SUM(player_bin_040) AS all_player_bin_040
, SUM(player_bin_050) AS all_player_bin_050
, SUM(player_bin_060) AS all_player_bin_060
, SUM(player_bin_070) AS all_player_bin_070
, SUM(player_bin_080) AS all_player_bin_080
, SUM(player_bin_090) AS all_player_bin_090
, SUM(player_bin_100) AS all_player_bin_100
, SUM(player_bin_100_plus) AS all_player_bin_100_plus

, SUM(player_bin_010::decimal) / SUM(tournament_count::decimal) AS all_player_bin_010_perc
, SUM(player_bin_020::decimal) / SUM(tournament_count::decimal) AS all_player_bin_020_perc
, SUM(player_bin_030::decimal) / SUM(tournament_count::decimal) AS all_player_bin_030_perc
, SUM(player_bin_040::decimal) / SUM(tournament_count::decimal) AS all_player_bin_040_perc
, SUM(player_bin_050::decimal) / SUM(tournament_count::decimal) AS all_player_bin_050_perc
, SUM(player_bin_060::decimal) / SUM(tournament_count::decimal) AS all_player_bin_060_perc
, SUM(player_bin_070::decimal) / SUM(tournament_count::decimal) AS all_player_bin_070_perc
, SUM(player_bin_080::decimal) / SUM(tournament_count::decimal) AS all_player_bin_080_perc
, SUM(player_bin_090::decimal) / SUM(tournament_count::decimal) AS all_player_bin_090_perc
, SUM(player_bin_100::decimal) / SUM(tournament_count::decimal) AS all_player_bin_100_perc
, SUM(player_bin_100_plus::decimal) / SUM(tournament_count::decimal) AS all_player_bin_100_plus_perc


FROM {{ ref('stg_geography_player_bins') }} stg_geography_player_bins
