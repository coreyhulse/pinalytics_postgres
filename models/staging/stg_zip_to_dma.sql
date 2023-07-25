SELECT
  zip_code::text AS zip_code
, MAX(dma_code) AS dma_code
, MAX(dma_description) AS dma_description
FROM {{ ref('zip_to_dma') }}
GROUP BY 1
