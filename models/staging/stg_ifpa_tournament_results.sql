SELECT
  tournament_result_id
, CAST(tournament_id AS int) AS tournament_id
, ranking_system
, CAST(player_count AS int) AS player_count
, CAST(CASE WHEN player_id = '' THEN NULL ELSE player_id END AS int) AS player_id
, first_name
, last_name
, profile_photo
, country_name
, country_code
, CAST(position AS int) AS position
, CAST(points AS decimal(9,2)) AS points
, CAST(wppr_rank AS int) AS wppr_rank
, CAST(CASE WHEN ratings_value = 'Not Rated' THEN NULL ELSE ratings_value END AS decimal(9,2)) AS ratings_value
, CAST(excluded_flag AS boolean) AS excluded_flag
, error_message
, extract_timestamp
, line_timestamp
FROM {{ source('pinalytics_raw', 'ifpa_tournament_results') }}


{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                      add PRIMARY KEY(tournament_result_id)
                    --, add INDEX index_tournament (tournament_id)
                    --, add INDEX index_player (player_id)'
    })
}}
