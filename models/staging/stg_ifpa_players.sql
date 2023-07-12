SELECT
  player_id
, first_name
, last_name
, initials
, gender
, CAST(CASE WHEN age = '' THEN NULL ELSE age END AS int) AS age
, CAST(CASE WHEN excluded_flag = '' THEN NULL ELSE excluded_flag END AS boolean) AS excluded_flag
, city
, stateprov
, country_name
, country_code
, CAST(CASE WHEN ifpa_registered = '' THEN NULL ELSE ifpa_registered END AS boolean) AS ifpa_registered
, profile_photo
, CAST(CASE WHEN current_wppr_rank = '' OR current_wppr_rank = 'None' THEN NULL ELSE current_wppr_rank END AS int) AS current_wppr_rank
, CAST(CASE WHEN last_month_rank = '' OR last_month_rank = 'None' THEN NULL ELSE last_month_rank END AS int) AS last_month_rank
, CAST(CASE WHEN last_year_rank = '' OR last_year_rank = 'None' THEN NULL ELSE last_year_rank END AS int) AS last_year_rank
, CAST(CASE WHEN highest_rank = '' OR highest_rank = 'None' THEN NULL ELSE highest_rank END AS int) AS highest_rank
, CAST(CASE WHEN highest_rank_date = '' OR highest_rank_date = '0000-00-00' OR highest_rank_date = 'None' THEN NULL ELSE highest_rank_date END AS date) AS highest_rank_date
, CAST(CASE WHEN current_wppr_points = '' OR current_wppr_points = 'None' THEN NULL ELSE current_wppr_points END AS decimal(9,2)) AS current_wppr_points
, CAST(CASE WHEN all_time_wppr_points = '' OR all_time_wppr_points = 'None' THEN NULL ELSE all_time_wppr_points END AS decimal(9,2)) AS all_time_wppr_points
, CAST(CASE WHEN best_finish = '' OR best_finish = 'None' THEN NULL ELSE best_finish END AS int) AS best_finish
, CAST(CASE WHEN best_finish_count = '' OR best_finish_count = 'None' THEN NULL ELSE best_finish_count END AS int) AS best_finish_count
, CAST(CASE WHEN average_finish = '' OR average_finish = 'None' THEN NULL ELSE average_finish END AS int) AS average_finish
, CAST(CASE WHEN average_finish_last_year = '' OR average_finish_last_year = 'None' THEN NULL ELSE average_finish_last_year END AS int) AS average_finish_last_year
, CAST(CASE WHEN total_events_all_time = '' OR total_events_all_time = 'None' THEN NULL ELSE total_events_all_time END AS int) AS total_events_all_time
, CAST(CASE WHEN total_active_events = '' OR total_active_events = 'None' THEN NULL ELSE total_active_events END AS int) AS total_active_events
, CAST(CASE WHEN total_events_away = '' OR total_events_away = 'None' THEN NULL ELSE total_events_away END AS int) AS total_events_away
, CAST(CASE WHEN ratings_rank = '' OR ratings_rank = 'Not Ranked' THEN NULL ELSE ratings_rank END AS int) AS ratings_rank
, CAST(CASE WHEN ratings_value = '' OR ratings_value = 'Not Ranked' THEN NULL ELSE ratings_value END AS decimal(9,2)) AS ratings_value
, CAST(CASE WHEN efficiency_rank = '' OR efficiency_rank = 'Not Ranked' THEN NULL ELSE efficiency_rank END AS int) AS efficiency_rank
, CAST(CASE WHEN efficiency_value = '' OR efficiency_value = 'Not Ranked' OR efficiency_value = 'NULL' THEN NULL ELSE efficiency_value END AS decimal(9,2)) AS efficiency_value
, CAST(CASE WHEN years_active = '' OR years_active = 'None' THEN NULL ELSE years_active END AS int) AS years_active
, error_message
, CASE WHEN error_message > '' THEN 0 ELSE 1 END AS is_valid
, extract_timestamp
, line_timestamp
FROM {{ source('pinalytics_raw', 'ifpa_players') }}
WHERE player_id <> 0

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add PRIMARY KEY(player_id)'
    })
}}
