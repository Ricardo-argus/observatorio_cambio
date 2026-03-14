WITH pct_venda as(
SELECT variacao_pct_venda, datahoracotacao FROM {{ source('silver', 'silver_dol_cambio') }}
)

SELECT AVG(variacao_pct_venda) as media_pct_venda FROM  pct_venda
WHERE EXTRACT(YEAR FROM datahoracotacao) < 2026