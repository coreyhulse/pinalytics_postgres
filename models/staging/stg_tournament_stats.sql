SELECT
  fct_ifpa_tournaments.tournament_id
, fct_ifpa_tournaments.tournament_name
, fct_ifpa_tournaments.tournament_type
, fct_ifpa_tournaments.prestige_flag
, fct_ifpa_tournaments.private_flag
, fct_ifpa_tournaments.address1
, fct_ifpa_tournaments.address2
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
, fct_ifpa_tournaments.director_name
, fct_ifpa_tournaments.director_id
, fct_ifpa_tournaments.website
, fct_ifpa_tournaments.event_name
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
, fct_ifpa_tournaments.date
, fct_ifpa_tournaments.ratings_strength
, fct_ifpa_tournaments.rankings_strength
, fct_ifpa_tournaments.base_value
, fct_ifpa_tournaments.tournament_percentage_grade
, fct_ifpa_tournaments.tournament_value
, fct_ifpa_tournaments.player_count
, fct_ifpa_tournaments.ranking_system
, fct_ifpa_tournaments.qualifying_format
, fct_ifpa_tournaments.finals_format
, stg_tournament_persona_summary.count_wpprtunist
, stg_tournament_persona_summary.count_traveler
, stg_tournament_persona_summary.count_localsupporter
, stg_tournament_persona_summary.count_onetimer
, stg_tournament_persona_summary.count_nonactive
, CASE
    WHEN stg_tournament_persona_summary.count_wpprtunist / fct_ifpa_tournaments.player_count > .6 THEN 'WPPRtunist Event'
    WHEN stg_tournament_persona_summary.count_traveler / fct_ifpa_tournaments.player_count > .3 THEN 'Traveler Event'
    WHEN stg_tournament_persona_summary.count_localsupporter / fct_ifpa_tournaments.player_count > .5 THEN 'Local Supporter Event'
    WHEN stg_tournament_persona_summary.count_onetimer / fct_ifpa_tournaments.player_count > .2 THEN 'One-Timer Event'
    WHEN stg_tournament_persona_summary.count_nonactive / fct_ifpa_tournaments.player_count > .5 THEN 'Non-Active Player Event'
    ELSE 'Mixed Persona Event'
  END AS event_persona

FROM {{ ref('fct_ifpa_tournaments') }} fct_ifpa_tournaments
LEFT JOIN {{ ref('stg_tournament_persona_summary') }} stg_tournament_persona_summary
ON fct_ifpa_tournaments.tournament_id = stg_tournament_persona_summary.tournament_id

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                      add PRIMARY KEY(tournament_id)
                    --, add INDEX index_date (date)
                    --, add INDEX index_geography (geography(255))
                    ',
    })
}}
