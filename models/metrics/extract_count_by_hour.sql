select
  date_trunc('hour', line_timestamp) AS extract_hour
, 'ifpa_tournament_results' AS source_table
, count(*) as line_count
from {{ ref('fct_ifpa_tournament_results') }}
group by 1

union all

select
  date_trunc('hour', line_timestamp) AS extract_hour
, 'ifpa_tournaments' AS source_table
, count(*) as line_count
from {{ ref('fct_ifpa_tournaments') }}
group by 1

union all

select
  date_trunc('hour', line_timestamp) AS extract_hour
, 'ifpa_players' AS source_table
, count(*) as line_count
from {{ ref('fct_ifpa_players') }}
group by 1
