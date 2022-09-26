SELECT
	  player_geography_highlights.rolling_48_geography_01
    , stg_player_by_geography_rank.rolling_48_travel_geographies
    , sum(stg_player_by_geography_rank.tournament_count_rolling_48) AS player_count_rolling_48
FROM {{ ref('player_geography_highlights') }} player_geography_highlights
LEFT JOIN (
	SELECT
		  stg_player_by_geography_rank.player_id
        , stg_player_by_geography_rank.geography AS rolling_48_travel_geographies
        , stg_player_by_geography_rank.tournament_count_rolling_48
	FROM {{ ref('stg_player_by_geography_rank') }} stg_player_by_geography_rank
    WHERE stg_player_by_geography_rank.tournament_count_rolling_48 > 0
    AND stg_player_by_geography_rank.tournament_count_rolling_48_rank <> 1
) AS stg_player_by_geography_rank
ON player_geography_highlights.player_id = stg_player_by_geography_rank.player_id
WHERE player_geography_highlights.rolling_48_geography_01 IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 3 DESC
