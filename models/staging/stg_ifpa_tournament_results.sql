SELECT
  tournament_id
, ranking_system
, player_count
, player_id
, first_name
, last_name
, profile_photo
, country_name
, country_code
, position
, points
, wppr_rank
, ratings_value
, excluded_flag
, error_message
, extract_timestamp
FROM {{ source('pinalytics_raw', 'ifpa_tournament_results') }}


{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                      add INDEX index_tournament (tournament_id)
                    , add INDEX index_player (player_id)'
    })
}}
