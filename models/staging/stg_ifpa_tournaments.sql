SELECT
  tournament_id
, tournament_name
, tournament_type
, prestige_flag
, private_flag
, address1
, address2
, CASE WHEN city > '' THEN city ELSE NULL END AS city
, CASE WHEN city > '' THEN 1 ELSE 0 END AS city_known
, CASE WHEN stateprov > '' THEN stateprov ELSE NULL END AS stateprov
, CASE WHEN stateprov > '' THEN 1 ELSE 0 END AS stateprov_known
, CONCAT(UPPER(city), ' ', UPPER(stateprov)) AS city_state
, CASE WHEN postal_code > '' THEN postal_code ELSE NULL END AS postal_code
, CASE WHEN postal_code > '' THEN 1 ELSE 0 END AS postal_code_known
, latitude
, longitude
, CASE WHEN country_name > '' THEN country_name ELSE NULL END AS country_name
, CASE WHEN country_code > '' THEN country_code ELSE NULL END AS country_code
, CASE WHEN country_code = 'US' THEN 1 ELSE 0 END AS country_us
, CASE WHEN country_code IS NOT NULL THEN 1 ELSE 0 END AS country_known
, director_name
, director_id
, website
, event_name
, event_start_date
, event_start_date AS date
, event_end_date
, ratings_strength
, rankings_strength
, base_value
, tournament_percentage_grade
, tournament_value
, details
, games_to_win
, qualify_flag
, qualify_hours
, unlimited_qualifying_flag
, eligible_player_count
, player_count
, ranking_system
, qualifying_format
, finals_format
, error_message
, CASE WHEN error_message > '' THEN 0 ELSE 1 END AS is_valid
, extract_timestamp
FROM {{ source('pinalytics_raw', 'ifpa_tournaments') }}
WHERE tournament_id <> 0

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                      add PRIMARY KEY(tournament_id)
                    --, add INDEX index_city_state ON (city_state(255))
                    --, add INDEX index_date ON (date)'
    })
}}
