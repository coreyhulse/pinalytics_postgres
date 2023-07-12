SELECT
  fct_ifpa_tournament_results.geography
, fct_ifpa_tournament_results.event_start_date AS date
, SUM(points) AS points
, SUM(CASE WHEN position = 1 THEN points ELSE NULL END) AS points_winner
, COUNT(DISTINCT tournament_id) AS tournaments
FROM {{ ref('fct_ifpa_tournament_results') }} fct_ifpa_tournament_results
GROUP BY 1,2

/*
        "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                        --  add INDEX index_geography (geography(255))
                        --, add INDEX index_date (date)'
*/
