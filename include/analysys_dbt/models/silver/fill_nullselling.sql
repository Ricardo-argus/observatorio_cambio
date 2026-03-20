SELECT {{null_prices('cotacao_venda', 0)}}
FROM {{source('silver', 'silver_dol_cambio')}}