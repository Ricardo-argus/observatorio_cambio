WITH variation_purchase_rate2025 AS(
    SELECT variacao_compra_moeda, datahoracotacao
     FROM {{ source('gold', 'cambio_eur_usd') }}
)

SELECT MAX(variacao_compra_moeda) FROM variation_purchase_rate2025
WHERE EXTRACT(YEAR FROM datahoracotacao) = 2025
