WITH selling_currency_2023_to_2025 AS (
    SELECT cotacao_venda_dol, datahoracotacao FROM {{source('gold', 'cambio_eur_usd')}}
)

SELECT AVG(cotacao_venda_dol) FROM selling_currency_2023_to_2025
WHERE EXTRACT(YEAR FROM datahoracotacao) >=2023