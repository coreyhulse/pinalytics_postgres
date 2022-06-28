SELECT DISTINCT
location
FROM {{ ref('ifpa_tournaments') }} ifpa_tournaments

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add PRIMARY KEY(location(255))'
    })
}}
