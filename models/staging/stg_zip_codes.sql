SELECT
    zip AS zip_code
  , type
  , decommissioned
  , primary_city AS city
  , state
  , county
  , CAST(CONCAT(UPPER(primary_city), ' ', UPPER(state)) AS CHAR(255)) AS city_state
  , timezone
--  , area_codes
  , world_region
  , country
  , latitude
  , longitude
FROM {{ ref('zip_codes') }}

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                      add PRIMARY KEY(zip_code)
                    --, add INDEX index_city_state (city_state(255))
                    '
    })
}}
