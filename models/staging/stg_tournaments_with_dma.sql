SELECT
  stg_tournaments_in_us_with_zip.tournament_id
, stg_tournaments_in_us_with_zip.postal_code
, zip_to_dma.zip_code
, zip_to_dma.dma_description
, zip_to_dma.dma_code
FROM {{ ref('stg_tournaments_in_us_with_zip') }} AS stg_tournaments_in_us_with_zip
LEFT JOIN {{ ref('stg_zip_to_dma') }} AS zip_to_dma
ON stg_tournaments_in_us_with_zip.postal_code = CAST(zip_to_dma.zip_code AS text)

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add PRIMARY KEY(tournament_id)'
    })
}}
