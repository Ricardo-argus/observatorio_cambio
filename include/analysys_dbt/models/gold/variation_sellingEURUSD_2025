WITH selling_EURUSD_variation_2025 AS(
    SELECT 
    variacao_venda_moeda, 
    datahoracotacao 
    FROM {{ source('gold', 'cambio_eur_usd') }}
)

SELECT AVG(variacao_venda_moeda) FROM selling_EURUSD_variation_2025
WHERE EXTRACT(YEAR FROM datahoracotacao)= 2025