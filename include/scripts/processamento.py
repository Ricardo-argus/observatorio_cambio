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

    df["variacao_venda"] = (df["cotacao_venda"] - df["cotacao_venda"].shift(1)).round(2)

    df["variacao_compra"] = (df["cotacao_compra"] - df["cotacao_compra"].shift(1).round(2))

    # Calcula variação percentual diária da cotação de venda
    df["variacao_pct_venda"] = (df["cotacao_venda"].pct_change() * 100).round(2)

    # Calcula variação percentual diária da cotação de compra
    df["variacao_pct_compra"] = (df["cotacao_compra"].pct_change() * 100).round(2)

    # Salva os resultados na tabela silver_cambio
    df.to_sql("silver_cambio", engine, if_exists="append", index=False)