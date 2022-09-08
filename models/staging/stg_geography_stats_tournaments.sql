SELECT
    stg_tournament_stats.geography
  , COUNT(DISTINCT CASE WHEN stg_tournament_stats.event_persona = 'WPPRtunist' THEN stg_tournament_stats.tournament_id ELSE NULL END) AS count_tournament_wpprtunist
  , COUNT(DISTINCT CASE WHEN stg_tournament_stats.event_persona = 'Traveler' THEN stg_tournament_stats.tournament_id ELSE NULL END) AS count_tournament_traveler
  , COUNT(DISTINCT CASE WHEN stg_tournament_stats.event_persona = 'Local Supporter' THEN stg_tournament_stats.tournament_id ELSE NULL END) AS count_tournament_localsupporter
  , COUNT(DISTINCT CASE WHEN stg_tournament_stats.event_persona = 'One-Timer' THEN stg_tournament_stats.tournament_id ELSE NULL END) AS count_tournament_onetimer
  , COUNT(DISTINCT CASE WHEN stg_tournament_stats.event_persona = 'Non-Active' THEN stg_tournament_stats.tournament_id ELSE NULL END) AS count_tournament_nonactive
  , COUNT(DISTINCT CASE WHEN stg_tournament_stats.event_persona = 'Mixed Persona' THEN stg_tournament_stats.tournament_id ELSE NULL END) AS count_tournament_mixedpersona
  , COUNT(DISTINCT stg_tournament_stats.tournament_id ) AS count_tournaments

FROM {{ ref('stg_tournament_stats') }} stg_tournament_stats
WHERE rolling_48_month = 1
GROUP BY 1


{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                      add PRIMARY KEY(geography(255))',
    })
}}
