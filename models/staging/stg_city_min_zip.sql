SELECT
    city_state
  , MIN(zip_code) AS zip_code
FROM {{ ref('stg_zip_codes') }}
GROUP BY 1


