SELECT
  stg_tournament_player_summary.geography
, COUNT(DISTINCT CASE WHEN stg_tournament_player_summary.player_persona = 'WPPRtunist' THEN stg_tournament_player_summary.player_id ELSE NULL END) AS count_player_wpprtunist
, COUNT(DISTINCT CASE WHEN stg_tournament_player_summary.player_persona = 'Traveler' THEN stg_tournament_player_summary.player_id ELSE NULL END) AS count_player_traveler
, COUNT(DISTINCT CASE WHEN stg_tournament_player_summary.player_persona = 'Local Supporter' THEN stg_tournament_player_summary.player_id ELSE NULL END) AS count_player_localsupporter
, COUNT(DISTINCT CASE WHEN stg_tournament_player_summary.player_persona = 'One-Timer' THEN stg_tournament_player_summary.player_id ELSE NULL END) AS count_player_onetimer
, COUNT(DISTINCT CASE WHEN stg_tournament_player_summary.player_persona = 'Non-Active' THEN stg_tournament_player_summary.player_id ELSE NULL END) AS count_player_nonactive
, COUNT(DISTINCT stg_tournament_player_summary.player_id) AS count_players
FROM {{ ref('stg_tournament_player_summary') }} stg_tournament_player_summary
WHERE rolling_48_month = 1
GROUP BY 1

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                    add PRIMARY KEY(geography)'
    })
}}
