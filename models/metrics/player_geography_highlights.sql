  SELECT
        stg_player_by_geography_rank.player_id
      , SUM(stg_player_by_geography_rank.tournament_count_rolling_12) AS tournament_count_rolling_12
      , SUM(stg_player_by_geography_rank.points_rolling_12) AS points_rolling_12
      , COUNT(DISTINCT CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_48_rank >= 1 THEN stg_player_by_geography_rank.geography ELSE NULL END) AS distinct_geography_count_rolling_12
      , MAX(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_12_rank = 1 THEN stg_player_by_geography_rank.geography ELSE NULL END) AS rolling_12_geography_01
      , SUM(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_12_rank = 1 THEN stg_player_by_geography_rank.tournament_count_rolling_12 ELSE NULL END) AS tournament_count_rolling_12_geography_01
      , SUM(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_12_rank = 1 THEN stg_player_by_geography_rank.points_rolling_12 ELSE NULL END) AS points_rolling_12_geography_01
      , MAX(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_12_rank = 2 THEN stg_player_by_geography_rank.geography ELSE NULL END) AS rolling_12_geography_02
      , MAX(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_12_rank = 3 THEN stg_player_by_geography_rank.geography ELSE NULL END) AS rolling_12_geography_03
      , MAX(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_12_rank = 4 THEN stg_player_by_geography_rank.geography ELSE NULL END) AS rolling_12_geography_04
      , SUM(stg_player_by_geography_rank.tournament_count_rolling_48) AS tournament_count_rolling_48
      , SUM(stg_player_by_geography_rank.points_rolling_48) AS points_rolling_48
      , COUNT(DISTINCT CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_48_rank >= 1 THEN stg_player_by_geography_rank.geography ELSE NULL END) AS distinct_geography_count_rolling_48
      , MAX(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_48_rank = 1 THEN stg_player_by_geography_rank.geography ELSE NULL END) AS rolling_48_geography_01
      , SUM(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_48_rank = 1 THEN stg_player_by_geography_rank.tournament_count_rolling_48 ELSE NULL END) AS tournament_count_rolling_48_geography_01
      , SUM(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_48_rank = 1 THEN stg_player_by_geography_rank.points_rolling_48 ELSE NULL END) AS points_rolling_48_geography_01
      , MAX(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_48_rank = 2 THEN stg_player_by_geography_rank.geography ELSE NULL END) AS rolling_48_geography_02
      , MAX(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_48_rank = 3 THEN stg_player_by_geography_rank.geography ELSE NULL END) AS rolling_48_geography_03
      , MAX(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_48_rank = 4 THEN stg_player_by_geography_rank.geography ELSE NULL END) AS rolling_48_geography_04
      , SUM(stg_player_by_geography_rank.points_rolling_all_time) AS points_rolling_all_time
      , COUNT(DISTINCT stg_player_by_geography_rank.geography) AS distinct_geography_count_rolling_all_time
      , MAX(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_all_time_rank = 1 THEN stg_player_by_geography_rank.geography ELSE NULL END) AS rolling_all_time_geography_01
      , SUM(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_all_time_rank = 1 THEN stg_player_by_geography_rank.tournament_count_rolling_all_time ELSE NULL END) AS tournament_count_rolling_all_time_geography_01
      , SUM(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_all_time_rank = 1 THEN stg_player_by_geography_rank.points_rolling_all_time ELSE NULL END) AS points_rolling_all_time_geography_01
      , MAX(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_all_time_rank = 2 THEN stg_player_by_geography_rank.geography ELSE NULL END) AS rolling_all_time_geography_02
      , MAX(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_all_time_rank = 3 THEN stg_player_by_geography_rank.geography ELSE NULL END) AS rolling_all_time_geography_03
      , MAX(CASE WHEN stg_player_by_geography_rank.tournament_count_rolling_all_time_rank = 4 THEN stg_player_by_geography_rank.geography ELSE NULL END) AS rolling_all_time_geography_04
  FROM {{ ref('stg_player_by_geography_rank') }} stg_player_by_geography_rank
  GROUP BY 1
  ORDER BY 1


 {{
   config({
     "pre-hook": 'SET @counter_01 = 0, @curr_player = 0',
     "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                       add INDEX index_player_id (player_id)'
     })
 }}
