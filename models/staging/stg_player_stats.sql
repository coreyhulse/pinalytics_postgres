SELECT
  fct_ifpa_players.player_id
, fct_ifpa_players.first_name
, fct_ifpa_players.last_name
, fct_ifpa_players.initials
, fct_ifpa_players.gender
, fct_ifpa_players.age
, fct_ifpa_players.excluded_flag
, fct_ifpa_players.city
, fct_ifpa_players.stateprov
, fct_ifpa_players.country_name
, fct_ifpa_players.country_code
, fct_ifpa_players.ifpa_registered
, fct_ifpa_players.profile_photo
, fct_ifpa_players.current_wppr_rank
, fct_ifpa_players.last_month_rank
, fct_ifpa_players.last_year_rank
, fct_ifpa_players.highest_rank
, fct_ifpa_players.highest_rank_date
, fct_ifpa_players.current_wppr_points
, fct_ifpa_players.all_time_wppr_points
, fct_ifpa_players.best_finish
, fct_ifpa_players.best_finish_count
, fct_ifpa_players.average_finish
, fct_ifpa_players.average_finish_last_year
, fct_ifpa_players.total_events_all_time
, fct_ifpa_players.total_active_events
, fct_ifpa_players.total_events_away
, fct_ifpa_players.ratings_rank
, fct_ifpa_players.ratings_value
, fct_ifpa_players.efficiency_rank
, fct_ifpa_players.efficiency_value
, fct_ifpa_players.years_active
, fct_ifpa_players.error_message
, fct_ifpa_players.extract_timestamp
, player_geography_highlights.tournament_count_rolling_12
, player_geography_highlights.points_rolling_12
, player_geography_highlights.distinct_geography_count_rolling_12
, player_geography_highlights.rolling_12_geography_01
, player_geography_highlights.tournament_count_rolling_12_geography_01
, player_geography_highlights.points_rolling_12_geography_01
, player_geography_highlights.rolling_12_geography_02
, player_geography_highlights.rolling_12_geography_03
, player_geography_highlights.rolling_12_geography_04
, player_geography_highlights.tournament_count_rolling_48
, player_geography_highlights.points_rolling_48
, player_geography_highlights.distinct_geography_count_rolling_48
, player_geography_highlights.rolling_48_geography_01
, player_geography_highlights.tournament_count_rolling_48_geography_01
, player_geography_highlights.points_rolling_48_geography_01
, player_geography_highlights.rolling_48_geography_02
, player_geography_highlights.rolling_48_geography_03
, player_geography_highlights.rolling_48_geography_04
, player_geography_highlights.points_rolling_all_time
, player_geography_highlights.distinct_geography_count_rolling_all_time
, player_geography_highlights.rolling_all_time_geography_01
, player_geography_highlights.tournament_count_rolling_all_time_geography_01
, player_geography_highlights.points_rolling_all_time_geography_01
, player_geography_highlights.rolling_all_time_geography_02
, player_geography_highlights.rolling_all_time_geography_03
, player_geography_highlights.rolling_all_time_geography_04
, stg_player_recency_flag.most_recent_tournament_date
, stg_player_recency_flag.rolling_01_month
, stg_player_recency_flag.rolling_03_month
, stg_player_recency_flag.rolling_12_month
, stg_player_recency_flag.rolling_24_month
, stg_player_recency_flag.rolling_36_month
, stg_player_recency_flag.rolling_48_month
, stg_player_recency_flag.rolling_60_month
, stg_player_recency_flag.rolling_all_time

, CASE
    WHEN distinct_geography_count_rolling_48 IS NULL THEN '00'
    WHEN distinct_geography_count_rolling_48 = 0 THEN '00'
    WHEN distinct_geography_count_rolling_48 = 1 THEN '01'
    WHEN distinct_geography_count_rolling_48 = 2 THEN '02'
    WHEN distinct_geography_count_rolling_48 = 3 THEN '03'
    WHEN distinct_geography_count_rolling_48 < 10 THEN '04 - 09'
    WHEN distinct_geography_count_rolling_48 >= 10 THEN '10+'
    ELSE '00'
  END AS distinct_geography_bin
, CASE
    WHEN tournament_count_rolling_48 IS NULL THEN '00'
    WHEN tournament_count_rolling_48 = 0 THEN '00'
    WHEN tournament_count_rolling_48 = 1 THEN '01'
    WHEN tournament_count_rolling_48 = 2 THEN '02'
    WHEN tournament_count_rolling_48 = 3 THEN '03'
    WHEN tournament_count_rolling_48 < 10 THEN '04 - 09'
    WHEN tournament_count_rolling_48 >= 10 THEN '10+'
    ELSE '00'
  END AS tournament_count_bin
, CASE
    WHEN years_active IS NULL THEN NULL
    WHEN years_active = 0 THEN '0+'
    WHEN years_active = 1 THEN '1+'
    WHEN years_active = 2 THEN '2+'
    WHEN years_active = 3 THEN '3+'
    WHEN years_active < 9 THEN '4+'
    WHEN years_active >= 9 THEN '9+'
    ELSE NULL
  END AS years_active_bin
, CASE
    WHEN tournament_count_rolling_48 = 0 THEN NULL
    WHEN tournament_count_rolling_48_geography_01 / tournament_count_rolling_48 IS NULL THEN '000%'
    WHEN tournament_count_rolling_48_geography_01 / tournament_count_rolling_48 < 0.20 THEN '000% - 020%'
    WHEN tournament_count_rolling_48_geography_01 / tournament_count_rolling_48 < 0.40 THEN '020% - 040%'
    WHEN tournament_count_rolling_48_geography_01 / tournament_count_rolling_48 < 0.60 THEN '040% - 060%'
    WHEN tournament_count_rolling_48_geography_01 / tournament_count_rolling_48 < 0.80 THEN '060% - 080%'
    WHEN tournament_count_rolling_48_geography_01 / tournament_count_rolling_48 < 1.00 THEN '080% - 100%'
    WHEN tournament_count_rolling_48_geography_01 / tournament_count_rolling_48 = 1.00 THEN '100%'
    ELSE '100%'
  END AS tournament_count_geo_01_perc_bin
, CASE
    WHEN points_rolling_48 = 0 THEN NULL
    WHEN points_rolling_48_geography_01 / points_rolling_48 IS NULL THEN '000%'
    WHEN points_rolling_48_geography_01 / points_rolling_48 < 0.20 THEN '000% - 020%'
    WHEN points_rolling_48_geography_01 / points_rolling_48 < 0.40 THEN '020% - 040%'
    WHEN points_rolling_48_geography_01 / points_rolling_48 < 0.60 THEN '040% - 060%'
    WHEN points_rolling_48_geography_01 / points_rolling_48 < 0.80 THEN '060% - 080%'
    WHEN points_rolling_48_geography_01 / points_rolling_48 < 1.00 THEN '080% - 100%'
    WHEN points_rolling_48_geography_01 / points_rolling_48 = 1.00 THEN '100%'
    ELSE '100%'
  END AS tournament_points_bin
, CASE
    WHEN current_wppr_rank IS NULL THEN NULL
    WHEN current_wppr_rank = 0 THEN NULL
    WHEN current_wppr_rank <= 100 THEN 100
    WHEN current_wppr_rank <= 1000 THEN 1000
    WHEN current_wppr_rank <= 2500 THEN 2500
    WHEN current_wppr_rank <= 5000 THEN 5000
    WHEN current_wppr_rank <= 10000 THEN 10000
    WHEN current_wppr_rank <= 50000 THEN 50000
    WHEN current_wppr_rank <= 100000 THEN 100000
    ELSE 1000000
  END AS ifpa_rank_bin
, CASE
    WHEN (distinct_geography_count_rolling_48 IS NULL OR distinct_geography_count_rolling_48 = 0) AND (tournament_count_rolling_48 IS NULL or tournament_count_rolling_48 = 0) THEN 'Non-Active'
    WHEN (distinct_geography_count_rolling_48 = 1) AND (tournament_count_rolling_48 = 1) THEN 'One-Timer'
    WHEN (distinct_geography_count_rolling_48 = 1) AND (tournament_count_rolling_48 >= 2) THEN 'Local Supporter'
    WHEN (distinct_geography_count_rolling_48 >= 2) AND (current_wppr_rank >= 1 AND current_wppr_rank <= 2500) THEN 'WPPRtunist'
    WHEN (distinct_geography_count_rolling_48 >= 2) AND (current_wppr_rank > 2500 OR current_wppr_rank = 0) THEN 'Traveler'
    ELSE 'Unknown'
  END AS player_persona

FROM {{ ref('fct_ifpa_players') }} fct_ifpa_players
LEFT JOIN {{ ref('player_geography_highlights') }} player_geography_highlights
ON fct_ifpa_players.player_id = player_geography_highlights.player_id
LEFT JOIN {{ ref('stg_player_recency_flag')}} stg_player_recency_flag
ON fct_ifpa_players.player_id = stg_player_recency_flag.player_id

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add PRIMARY KEY(player_id)'
    })
}}
