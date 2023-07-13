SELECT
  fct_ifpa_tournament_results.tournament_id
, fct_ifpa_tournament_results.player_id
, fct_ifpa_tournament_results.geography
, fct_ifpa_tournament_results.rolling_48_month
, stg_player_stats.player_persona
FROM {{ ref('fct_ifpa_tournament_results') }} fct_ifpa_tournament_results
LEFT JOIN {{ ref('stg_player_stats') }} stg_player_stats
ON fct_ifpa_tournament_results.player_id = stg_player_stats.player_id

/*
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                --    add INDEX index_player (player_id)
                --  , add INDEX index_tournament (tournament_id)
                --  , add INDEX index_geography (geography(255))
                '
    })
*/
