SELECT
  player_id
, first_name
, last_name
, initials
, gender
, age
, excluded_flag
, city
, stateprov
, country_name
, country_code
, ifpa_registered
, profile_photo
, current_wppr_rank
, last_month_rank
, last_year_rank
, highest_rank
, highest_rank_date
, current_wppr_points
, all_time_wppr_points
, best_finish
, best_finish_count
, average_finish
, average_finish_last_year
, total_events_all_time
, total_active_events
, total_events_away
, ratings_rank
, ratings_value
, efficiency_rank
, efficiency_value
, years_active
, error_message
, CASE WHEN error_message > '' THEN 0 ELSE 1 END AS is_valid
, extract_timestamp
FROM {{ source('pinalytics_raw', 'ifpa_players') }}
WHERE player_id <> 0

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add PRIMARY KEY(player_id)'
    })
}}
