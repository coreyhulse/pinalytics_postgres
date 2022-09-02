SELECT
  stg_ifpa_tournament_results.tournament_id
, stg_ifpa_tournament_results.ranking_system
, stg_ifpa_tournament_results.player_count AS tournament_player_count
, stg_ifpa_tournament_results.player_id
, stg_ifpa_tournament_results.first_name
, stg_ifpa_tournament_results.last_name
, stg_ifpa_tournament_results.profile_photo
, ifpa_tournaments.city
, ifpa_tournaments.city_known
, ifpa_tournaments.stateprov
, ifpa_tournaments.stateprov_known
, ifpa_tournaments.city_state
, ifpa_tournaments.postal_code_source
, ifpa_tournaments.postal_code
, ifpa_tournaments.postal_code_known
, ifpa_tournaments.dma_description
, ifpa_tournaments.geography
, ifpa_tournaments.country_name
, ifpa_tournaments.country_code
, ifpa_tournaments.country_us
, ifpa_tournaments.country_known
, ifpa_tournaments.event_name
, ifpa_tournaments.event_start_date
, ifpa_tournaments.date
, ifpa_tournaments.year
, ifpa_tournaments.yearmonth
, ifpa_tournaments.rolling_01_month
, ifpa_tournaments.rolling_03_month
, ifpa_tournaments.rolling_12_month
, ifpa_tournaments.rolling_24_month
, ifpa_tournaments.rolling_36_month
, ifpa_tournaments.rolling_48_month
, ifpa_tournaments.rolling_60_month
, ifpa_tournaments.rolling_all_time
, ifpa_tournaments.ratings_strength
, ifpa_tournaments.rankings_strength
, ifpa_tournaments.base_value
, ifpa_tournaments.tournament_percentage_grade
, ifpa_tournaments.tournament_value
, ifpa_tournaments.qualifying_format
, ifpa_tournaments.finals_format
, stg_ifpa_tournament_results.position
, stg_ifpa_tournament_results.position / stg_ifpa_tournament_results.player_count AS position_percentage
, stg_ifpa_tournament_results.points
, stg_ifpa_tournament_results.points / ifpa_tournaments.tournament_value AS points_percentage
, stg_ifpa_tournament_results.wppr_rank AS tournament_start_wppr_rank
, stg_ifpa_tournament_results.ratings_value AS tournament_start_ratings_value
, stg_ifpa_tournament_results.excluded_flag
, stg_ifpa_tournament_results.error_message
, stg_ifpa_tournament_results.extract_timestamp
FROM {{ ref('stg_ifpa_tournament_results') }} stg_ifpa_tournament_results
LEFT JOIN {{ ref('ifpa_tournaments') }} ifpa_tournaments
ON stg_ifpa_tournament_results.tournament_id = ifpa_tournaments.tournament_id

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                      add INDEX index_tournament (tournament_id)
                    , add INDEX index_date (date)
                    , add INDEX index_player (player_id)
                    , add INDEX index_geography (geography(255))'
    })
}}
