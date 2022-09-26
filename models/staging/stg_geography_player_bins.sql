SELECT
  fct_ifpa_tournaments.geography
, COUNT(DISTINCT tournament_id) AS tournament_count
, COUNT(DISTINCT CASE WHEN player_count_bin = 10 THEN tournament_id ELSE NULL END) AS player_bin_010
, COUNT(DISTINCT CASE WHEN player_count_bin = 20 THEN tournament_id ELSE NULL END) AS player_bin_020
, COUNT(DISTINCT CASE WHEN player_count_bin = 30 THEN tournament_id ELSE NULL END) AS player_bin_030
, COUNT(DISTINCT CASE WHEN player_count_bin = 40 THEN tournament_id ELSE NULL END) AS player_bin_040
, COUNT(DISTINCT CASE WHEN player_count_bin = 50 THEN tournament_id ELSE NULL END) AS player_bin_050
, COUNT(DISTINCT CASE WHEN player_count_bin = 60 THEN tournament_id ELSE NULL END) AS player_bin_060
, COUNT(DISTINCT CASE WHEN player_count_bin = 70 THEN tournament_id ELSE NULL END) AS player_bin_070
, COUNT(DISTINCT CASE WHEN player_count_bin = 80 THEN tournament_id ELSE NULL END) AS player_bin_080
, COUNT(DISTINCT CASE WHEN player_count_bin = 90 THEN tournament_id ELSE NULL END) AS player_bin_090
, COUNT(DISTINCT CASE WHEN player_count_bin = 100 THEN tournament_id ELSE NULL END) AS player_bin_100
, COUNT(DISTINCT CASE WHEN player_count_bin > 100 THEN tournament_id ELSE NULL END) AS player_bin_100_plus

, COUNT(DISTINCT CASE WHEN player_count_bin = 10 THEN tournament_id ELSE NULL END) / COUNT(DISTINCT tournament_id) AS player_bin_010_perc
, COUNT(DISTINCT CASE WHEN player_count_bin = 20 THEN tournament_id ELSE NULL END) / COUNT(DISTINCT tournament_id) AS player_bin_020_perc
, COUNT(DISTINCT CASE WHEN player_count_bin = 30 THEN tournament_id ELSE NULL END) / COUNT(DISTINCT tournament_id) AS player_bin_030_perc
, COUNT(DISTINCT CASE WHEN player_count_bin = 40 THEN tournament_id ELSE NULL END) / COUNT(DISTINCT tournament_id) AS player_bin_040_perc
, COUNT(DISTINCT CASE WHEN player_count_bin = 50 THEN tournament_id ELSE NULL END) / COUNT(DISTINCT tournament_id) AS player_bin_050_perc
, COUNT(DISTINCT CASE WHEN player_count_bin = 60 THEN tournament_id ELSE NULL END) / COUNT(DISTINCT tournament_id) AS player_bin_060_perc
, COUNT(DISTINCT CASE WHEN player_count_bin = 70 THEN tournament_id ELSE NULL END) / COUNT(DISTINCT tournament_id) AS player_bin_070_perc
, COUNT(DISTINCT CASE WHEN player_count_bin = 80 THEN tournament_id ELSE NULL END) / COUNT(DISTINCT tournament_id) AS player_bin_080_perc
, COUNT(DISTINCT CASE WHEN player_count_bin = 90 THEN tournament_id ELSE NULL END) / COUNT(DISTINCT tournament_id) AS player_bin_090_perc
, COUNT(DISTINCT CASE WHEN player_count_bin = 100 THEN tournament_id ELSE NULL END) / COUNT(DISTINCT tournament_id) AS player_bin_100_perc
, COUNT(DISTINCT CASE WHEN player_count_bin > 100 THEN tournament_id ELSE NULL END) / COUNT(DISTINCT tournament_id) AS player_bin_100_plus_perc

FROM {{ ref('fct_ifpa_tournaments') }} fct_ifpa_tournaments
WHERE rolling_48_month = 1
AND tournament_value > 0
GROUP BY 1




{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add PRIMARY KEY(geography(255))'
    })
}}
