  SELECT
        player_geography_highlights.rolling_48_geography_01 AS geography
      , COUNT(player_geography_highlights.player_id) AS player_count_top_geography_rolling_48
  FROM {{ ref('player_geography_highlights') }} player_geography_highlights
  GROUP BY 1
  ORDER BY 1


 {{
   config({
     "pre-hook": 'SET @counter_01 = 0, @curr_player = 0',
     "post-hook": '--ALTER TABLE {{ target.schema }}.{{ this.name }}
                   --    add INDEX geography (geography(255))
                   '
     })
 }}
