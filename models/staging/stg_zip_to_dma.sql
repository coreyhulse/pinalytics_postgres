SELECT
  zip_code
, MAX(dma_code) AS dma_code
, MAX(dma_description) AS dma_description
FROM {{ ref('zip_to_dma') }}
GROUP BY 1

{{
  config({
    "post-hook": 'ALTER TABLE {{ target.schema }}.{{ this.name }} add PRIMARY KEY(zip_code)'
    })
}}
