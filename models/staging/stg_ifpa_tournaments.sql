SELECT
  tournament_id
, tournament_name
, tournament_type
, CAST(CASE WHEN prestige_flag = '' THEN NULL ELSE prestige_flag END AS boolean) as prestige_flag
, CAST(CASE WHEN private_flag = '' THEN NULL ELSE private_flag END AS boolean) as private_flag
, address1
, address2
, CASE WHEN city > '' THEN city ELSE NULL END AS city
, CASE WHEN city > '' THEN 1 ELSE 0 END AS city_known
, CASE WHEN stateprov > '' THEN stateprov ELSE NULL END AS stateprov
, CASE WHEN stateprov > '' THEN 1 ELSE 0 END AS stateprov_known
, CONCAT(UPPER(city), ' ', UPPER(stateprov)) AS city_state
, CASE WHEN postal_code > '' THEN postal_code ELSE NULL END AS postal_code
, CASE WHEN postal_code > '' THEN 1 ELSE 0 END AS postal_code_known
, CAST(CASE WHEN latitude = '' THEN NULL ELSE latitude END AS decimal(9,4)) AS latitude
, CAST(CASE WHEN longitude = '' THEN NULL ELSE longitude END AS decimal(9,4)) AS longitude
, CASE WHEN country_name > '' THEN country_name ELSE NULL END AS country_name
, CASE WHEN country_code > '' THEN country_code ELSE NULL END AS country_code
, CASE WHEN country_code = 'US' THEN 1 ELSE 0 END AS country_us
, CASE WHEN country_code IS NOT NULL THEN 1 ELSE 0 END AS country_known
, director_name
, director_id AS director_id
, website
, event_name
, CAST(CASE WHEN event_start_date = '' OR event_start_date = '0000-00-00' THEN NULL ELSE event_start_date END AS date) AS event_start_date
, CAST(CASE WHEN event_end_date = '' OR event_end_date = '0000-00-00' THEN NULL ELSE event_end_date END AS date) AS event_end_date
, CAST(CASE WHEN event_end_date = '' OR event_end_date = '0000-00-00' THEN NULL ELSE event_end_date END AS date) AS date
, CAST(CASE WHEN ratings_strength = '' THEN NULL ELSE ratings_strength END AS decimal(9,2)) AS ratings_strength
, CAST(CASE WHEN rankings_strength = '' THEN NULL ELSE rankings_strength END AS decimal(9,2)) AS rankings_strength
, CAST(CASE WHEN base_value = '' THEN NULL ELSE base_value END AS decimal(9,4)) AS base_value
, CAST(CASE WHEN tournament_percentage_grade = '' THEN NULL ELSE tournament_percentage_grade END AS decimal(9,2)) AS tournament_percentage_grade
, CAST(CASE WHEN tournament_value = '' OR tournament_value = 'None' THEN NULL ELSE tournament_value END AS decimal(9,4)) AS tournament_value
, details
, CAST(CASE WHEN games_to_win = '' THEN NULL ELSE games_to_win END AS int) AS games_to_win
, CAST(CASE WHEN qualify_flag = '' THEN NULL ELSE qualify_flag END AS boolean) AS qualify_flag
, CAST(CASE WHEN qualify_hours = '' THEN NULL ELSE qualify_hours END AS int) AS qualify_hours
, CAST(CASE WHEN unlimited_qualifying_flag = '' THEN NULL ELSE unlimited_qualifying_flag END AS boolean) AS unlimited_qualifying_flag
, CAST(CASE WHEN eligible_player_count = '' THEN NULL ELSE eligible_player_count END AS int) AS eligible_player_count
, CAST(CASE WHEN player_count = '' THEN NULL ELSE player_count END AS int) AS player_count
, ranking_system
, qualifying_format
, finals_format
, error_message
, CASE WHEN error_message > '' THEN 0 ELSE 1 END AS is_valid
, extract_timestamp
, line_timestamp
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
