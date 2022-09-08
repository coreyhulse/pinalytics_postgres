SELECT
  ifpa_tournaments.tournament_id
, ifpa_tournaments.tournament_name
, ifpa_tournaments.tournament_type
, ifpa_tournaments.prestige_flag
, ifpa_tournaments.private_flag
, ifpa_tournaments.address1
, ifpa_tournaments.address2
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
, ifpa_tournaments.director_name
, ifpa_tournaments.director_id
, ifpa_tournaments.website
, ifpa_tournaments.event_name
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
, ifpa_tournaments.date
, ifpa_tournaments.ratings_strength
, ifpa_tournaments.rankings_strength
, ifpa_tournaments.base_value
, ifpa_tournaments.tournament_percentage_grade
, ifpa_tournaments.tournament_value
, ifpa_tournaments.player_count
, ifpa_tournaments.ranking_system
, ifpa_tournaments.qualifying_format
, ifpa_tournaments.finals_format
, stg_tournament_persona_summary.count_wpprtunist
, stg_tournament_persona_summary.count_traveler
, stg_tournament_persona_summary.count_localsupporter
, stg_tournament_persona_summary.count_onetimer
, stg_tournament_persona_summary.count_nonactive
, CASE
    WHEN stg_tournament_persona_summary.count_wpprtunist / ifpa_tournaments.player_count > .6 THEN 'WPPRtunist Event'
    WHEN stg_tournament_persona_summary.count_traveler / ifpa_tournaments.player_count > .3 THEN 'Traveler Event'
    WHEN stg_tournament_persona_summary.count_localsupporter / ifpa_tournaments.player_count > .5 THEN 'Local Supporter Event'
    WHEN stg_tournament_persona_summary.count_onetimer / ifpa_tournaments.player_count > .2 THEN 'One-Timer Event'
    WHEN stg_tournament_persona_summary.count_nonactive / ifpa_tournaments.player_count > .5 THEN 'Non-Active Player Event'
    ELSE 'Mixed Persona Event'
  END AS event_persona

FROM {{ ref('ifpa_tournaments') }} ifpa_tournaments
LEFT JOIN {{ ref('stg_tournament_persona_summary') }} stg_tournament_persona_summary
ON ifpa_tournaments.tournament_id = stg_tournament_persona_summary.tournament_id

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                      add PRIMARY KEY(tournament_id)
                    , add INDEX index_date (date)
                    , add INDEX index_geography (geography(255))',
    })
}}
