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
