# Task 2: Pandas + NumPy
import os 
import pandas as pd
import numpy as np
import sqlalchemy as sa

CONN_STR = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)
engine = sa.create_engine(CONN_STR)

def process_data():
    # Lê os dados da tabela bronze_cambio
    df = pd.read_sql("SELECT * FROM public.bronze_cambio", engine)

    # Calcula média móvel da cotação de venda (7 dias)
    df["media_movel_venda"] = df["cotacao_venda"].rolling(window=7).mean()

    # Calcula média móvel da cotação de compra (7 dias)
    df["media_movel_compra"] = df["cotacao_compra"].rolling(window=7).mean()

    # Calcula variação percentual diária da cotação de venda
    df["variacao_pct_venda"] = df["cotacao_venda"].pct_change() * 100

    # Calcula variação percentual diária da cotação de compra
    df["variacao_pct_compra"] = df["cotacao_compra"].pct_change() * 100

    # Salva os resultados na tabela silver_cambio
    df.to_sql("silver_cambio", engine, if_exists="replace", index=False)