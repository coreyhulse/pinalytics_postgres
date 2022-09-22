SELECT
  stg_ifpa_tournaments.tournament_id
, stg_ifpa_tournaments.tournament_name
, stg_ifpa_tournaments.tournament_type
, stg_ifpa_tournaments.prestige_flag
, stg_ifpa_tournaments.private_flag
, stg_ifpa_tournaments.address1
, stg_ifpa_tournaments.address2
, stg_ifpa_tournaments.city
, stg_ifpa_tournaments.city_known
, stg_ifpa_tournaments.stateprov
, stg_ifpa_tournaments.stateprov_known
, stg_ifpa_tournaments.city_state
, stg_ifpa_tournaments.postal_code AS postal_code_source
, COALESCE(stg_ifpa_tournaments.postal_code, stg_city_min_zip.zip_code) AS postal_code
, stg_ifpa_tournaments.postal_code_known
, COALESCE(stg_tournaments_with_dma.dma_description, zip_to_dma.dma_description) AS dma_description
, CASE
    WHEN country_code = 'US' THEN CONCAT('US - ', COALESCE(COALESCE(stg_tournaments_with_dma.dma_description, zip_to_dma.dma_description), stg_ifpa_tournaments.stateprov, 'UNKNOWN'))
    WHEN stateprov_known = 1 THEN UPPER(CONCAT(country_code, ' - ', stateprov))
    WHEN country_name > '' THEN UPPER(country_code)
    ELSE 'UNKNOWN'
  END AS geography
, stg_ifpa_tournaments.latitude
, stg_ifpa_tournaments.longitude
, stg_ifpa_tournaments.country_name
, stg_ifpa_tournaments.country_code
, stg_ifpa_tournaments.country_us
, stg_ifpa_tournaments.country_known
, stg_ifpa_tournaments.director_name
, stg_ifpa_tournaments.director_id
, stg_ifpa_tournaments.website
, stg_ifpa_tournaments.event_name
, stg_ifpa_tournaments.event_start_date
, stg_calendar.year
, stg_calendar.yearmonth
, stg_calendar.rolling_01_month
, stg_calendar.rolling_03_month
, stg_calendar.rolling_12_month
, stg_calendar.rolling_24_month
, stg_calendar.rolling_36_month
, stg_calendar.rolling_48_month
, stg_calendar.rolling_60_month
, stg_calendar.rolling_all_time
, stg_ifpa_tournaments.event_end_date
, stg_ifpa_tournaments.date
, stg_ifpa_tournaments.ratings_strength
, stg_ifpa_tournaments.rankings_strength
, stg_ifpa_tournaments.base_value
, stg_ifpa_tournaments.tournament_percentage_grade
, stg_ifpa_tournaments.tournament_value
, stg_ifpa_tournaments.details
, stg_ifpa_tournaments.games_to_win
, stg_ifpa_tournaments.qualify_flag
, stg_ifpa_tournaments.qualify_hours
, stg_ifpa_tournaments.unlimited_qualifying_flag
, stg_ifpa_tournaments.eligible_player_count
, stg_ifpa_tournaments.player_count
, CEILING(stg_ifpa_tournaments.player_count/10)*10 AS player_count_bin
, stg_ifpa_tournaments.ranking_system
, stg_ifpa_tournaments.qualifying_format
, stg_ifpa_tournaments.finals_format
, stg_ifpa_tournaments.error_message
, stg_ifpa_tournaments.extract_timestamp
FROM {{ ref('stg_ifpa_tournaments') }} AS stg_ifpa_tournaments
LEFT JOIN {{ ref('stg_tournaments_with_dma') }} AS stg_tournaments_with_dma
ON stg_ifpa_tournaments.tournament_id = stg_tournaments_with_dma.tournament_id
LEFT JOIN {{ ref('stg_city_min_zip') }} AS stg_city_min_zip
ON stg_ifpa_tournaments.city_state = stg_city_min_zip.city_state
LEFT JOIN {{ ref('stg_zip_to_dma') }} AS zip_to_dma
ON stg_city_min_zip.zip_code = zip_to_dma.zip_code
LEFT JOIN {{ ref('stg_calendar') }} stg_calendar
ON stg_ifpa_tournaments.date = stg_calendar.date
WHERE is_valid = 1


{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                      add PRIMARY KEY(tournament_id)
                    , add INDEX index_date (date)',
    })
}}
