import os
import pandas as pd
import numpy as np
import sqlalchemy as sa
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert

CONN_STR = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)
engine = sa.create_engine(CONN_STR)

def joins_cambio():
    df_dol = pd.read_sql("SELECT * FROM silver_dol_cambio", engine)
    df_euro = pd.read_sql("SELECT * FROM silver_euro_cambio", engine)

    df_variacoes = pd.merge(df_dol, df_euro, on='id', how='inner', suffixes=("_dol", "_euro"))

    df_variacoes = df_variacoes[
    ["id", "cotacao_venda_dol", "cotacao_venda_euro",
     "cotacao_compra_dol", "cotacao_compra_euro", "datahoracotacao_dol"]
    ]

    df_variacoes = df_variacoes.rename(columns={"datahoracotacao_dol": "datacotacao"})
    
    df_variacoes["datacotacao"] = pd.to_datetime(df_variacoes["datacotacao"]).dt.date

    numericos = ["cotacao_venda_dol", "cotacao_venda_euro","cotacao_compra_dol", "cotacao_compra_euro"]
    df_variacoes[numericos] = df_variacoes[numericos].round(3)

    df_variacoes["variacao_compra_moeda"] = (df_variacoes["cotacao_compra_dol"] - df_variacoes["cotacao_compra_euro"].shift(1)).abs().round(4)

    df_variacoes["variacao_venda_moeda"] = (df_variacoes["cotacao_venda_dol"] - df_variacoes["cotacao_venda_euro"].shift(1)).abs().round(4)

    df_variacoes.to_sql("cambio_EUR_USD", engine, if_exists='replace', index=False)

