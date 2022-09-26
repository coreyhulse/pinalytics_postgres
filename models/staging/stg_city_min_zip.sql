SELECT
    city_state
  , MIN(zip_code) AS zip_code
FROM {{ ref('stg_zip_codes') }}
GROUP BY 1

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }}
                      add INDEX index_city_state (city_state(255))
                    , add INDEX index_zip_code (zip_code)'
    })
}}
