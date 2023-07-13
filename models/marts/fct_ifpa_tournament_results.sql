SELECT
  stg_ifpa_tournament_results.tournament_id
, stg_ifpa_tournament_results.ranking_system
, stg_ifpa_tournament_results.player_count AS tournament_player_count
, stg_ifpa_tournament_results.player_id
, stg_ifpa_tournament_results.tournament_result_id AS tournament_player_row_id
, stg_ifpa_tournament_results.tournament_result_id
, stg_ifpa_tournament_results.first_name
, stg_ifpa_tournament_results.last_name
, stg_ifpa_tournament_results.profile_photo
, fct_ifpa_tournaments.city
, fct_ifpa_tournaments.city_known
, fct_ifpa_tournaments.stateprov
, fct_ifpa_tournaments.stateprov_known
, fct_ifpa_tournaments.city_state
, fct_ifpa_tournaments.postal_code_source
, fct_ifpa_tournaments.postal_code
, fct_ifpa_tournaments.postal_code_known
, fct_ifpa_tournaments.dma_description
, fct_ifpa_tournaments.geography
, fct_ifpa_tournaments.country_name
, fct_ifpa_tournaments.country_code
, fct_ifpa_tournaments.country_us
, fct_ifpa_tournaments.country_known
, fct_ifpa_tournaments.event_name
, fct_ifpa_tournaments.event_start_date
, fct_ifpa_tournaments.date
, fct_ifpa_tournaments.year
, fct_ifpa_tournaments.yearmonth
, fct_ifpa_tournaments.rolling_01_month
, fct_ifpa_tournaments.rolling_03_month
, fct_ifpa_tournaments.rolling_12_month
, fct_ifpa_tournaments.rolling_24_month
, fct_ifpa_tournaments.rolling_36_month
, fct_ifpa_tournaments.rolling_48_month
, fct_ifpa_tournaments.rolling_60_month
, fct_ifpa_tournaments.rolling_all_time
, fct_ifpa_tournaments.ratings_strength
, fct_ifpa_tournaments.rankings_strength
, fct_ifpa_tournaments.base_value
, fct_ifpa_tournaments.tournament_percentage_grade
, fct_ifpa_tournaments.tournament_value
, fct_ifpa_tournaments.qualifying_format
, fct_ifpa_tournaments.finals_format
, stg_ifpa_tournament_results.position
, stg_ifpa_tournament_results.position / NULLIF(stg_ifpa_tournament_results.player_count,0) AS position_percentage
, stg_ifpa_tournament_results.points
, stg_ifpa_tournament_results.points / NULLIF(fct_ifpa_tournaments.tournament_value,0) AS points_percentage
, stg_ifpa_tournament_results.wppr_rank AS tournament_start_wppr_rank
, stg_ifpa_tournament_results.ratings_value AS tournament_start_ratings_value
, stg_ifpa_tournament_results.excluded_flag
, stg_ifpa_tournament_results.error_message
, stg_ifpa_tournament_results.extract_timestamp
, stg_ifpa_tournament_results.line_timestamp
FROM {{ ref('stg_ifpa_tournament_results') }} stg_ifpa_tournament_results
LEFT JOIN {{ ref('fct_ifpa_tournaments') }} fct_ifpa_tournaments
ON stg_ifpa_tournament_results.tournament_id = fct_ifpa_tournaments.tournament_id

/*
    "pre-hook": 'SET @row_number = 0, @row_number = 0',
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                      add INDEX index_tournament (tournament_id)
                    , add INDEX index_date (date)
                    , add INDEX index_player (player_id)
                    , add INDEX index_geography (geography(255))'
*/