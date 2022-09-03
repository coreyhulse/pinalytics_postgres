SELECT
    stg_player_by_geography_rank.player_id
  , ifpa_players.first_name
  , ifpa_players.last_name
  , ifpa_players.current_wppr_rank
  , stg_player_by_geography_rank.geography
  , stg_player_by_geography_rank.tournament_count_rolling_12
  , stg_player_by_geography_rank.points_rolling_12
  , stg_player_by_geography_rank.tournament_count_rolling_48
  , stg_player_by_geography_rank.points_rolling_48
FROM {{ ref('stg_player_by_geography_rank') }} stg_player_by_geography_rank
LEFT JOIN {{ ref('ifpa_players') }} ifpa_players
ON ifpa_players.player_id = stg_player_by_geography_rank.player_id

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                      add INDEX index_geography (geography(255))
                    , add INDEX index_player_id (player_id)'
    })
}}
