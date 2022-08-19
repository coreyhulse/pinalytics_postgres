SELECT
  ifpa_tournament_results.geography
, ifpa_tournament_results.event_start_date AS date
, SUM(points) AS points
, SUM(CASE WHEN position = 1 THEN points ELSE NULL END) AS points_winner
, COUNT(DISTINCT tournament_id) AS tournaments
FROM {{ ref('ifpa_tournament_results') }} ifpa_tournament_results
GROUP BY 1,2

{{
  config({
        "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add INDEX index_geography (geography(255))',
        "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add INDEX index_date (date)'
    })
}}
