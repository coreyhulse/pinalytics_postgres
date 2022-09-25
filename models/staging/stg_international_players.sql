SELECT
      COUNT(DISTINCT player_id) AS all_player_count_rolling_48
    , SUM(points_rolling_48) AS all_player_points_rolling_48
FROM {{ ref('player_geography_highlights') }} player_geography_highlights
WHERE tournament_count_rolling_48 > 0
